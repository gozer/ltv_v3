import argparse
import logging
import dill
import time
#import math
from datetime import date #, timedelta, datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
#from apache_beam.io.parquetio import WriteToParquet # supposedly merged pre-beam 2.9
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

#import pandas as pd
#import numpy as np
from lifetimes import BetaGeoFitter
from lifetimes import GammaGammaFitter
from numpy import ceil, power

# initialize BigQuery client
from google.cloud import bigquery
bq_client = bigquery.Client()

#initialize beam's gs client
from apache_beam.io.gcp import gcsio
gcs = gcsio.GcsIO()

# Instantiates a client
import google.cloud.logging
client = google.cloud.logging.Client()
client.setup_logging()

import logging

logger = logging.getLogger(__name__)
logger.info('start logging')


class TestFn(beam.DoFn):

  def process(self, element):
    from google.cloud import bigquery
    bq_client = bigquery.Client()

    qry = ("SELECT count(*) as cts FROM ltv.summary")
    query_job = bq_client.query(qry)
    df_random_sample = query_job.to_dataframe() # no need to go through query_job.result()

    with gcs.open('gs://ltv-dataflow/tmp/testfn_dump.txt', 'wb') as out_file:
      dill.dump(df_random_sample.cts[0], out_file)
      
    logging.info('TestFn cts: {}'.format(df_random_sample.cts[0]))

    

class EstimateLTVFn(beam.DoFn):
  """A DoFn that estimates LTV model parameters given sample size and outputs to gs_bucket. """
  def __init__(self, sample_size, output_bucket='gs://ltv-dataflow/tmp/', model_penalizer=None):
    self.sample_size = sample_size
    self.output_bucket = output_bucket
    if model_penalizer is None:
      model_penalizer = 0
    self.model_penalizer = model_penalizer
    
  def process(self, element):
    from lifetimes import BetaGeoFitter
    from lifetimes import GammaGammaFitter
    #from pandas import Dataframe
    from google.cloud import bigquery
    bq_client = bigquery.Client()
    from apache_beam.io.gcp import gcsio
    gcs = gcsio.GcsIO()
    import dill
	
    logging.info('EstimateLTVFn on sample size: {}'.format(self.sample_size))

    qry = ("SELECT client_id, frequency, recency, T, monetary_value FROM ltv.summary ORDER BY RAND() LIMIT {}").format(self.sample_size)

    query_job = bq_client.query(qry)
    df_random_sample = query_job.to_dataframe() # no need to go through query_job.result()
    logging.info(df_random_sample.head(1))
  
    # Building the Model using BG/NBD
    bgf = BetaGeoFitter(penalizer_coef=self.model_penalizer)
    logging.info(type(df_random_sample))
    bgf.fit(df_random_sample['frequency'], df_random_sample['recency'], df_random_sample['T'])
    
    # There cannot be non-positive values in the monetary_value or frequency vector
    summary_with_value_and_returns = df_random_sample[(df_random_sample['monetary_value']>0) & (df_random_sample['frequency']>0)]
    # Setting up Gamma Gamma model
    ggf = GammaGammaFitter(penalizer_coef = 0)
    ggf.fit(summary_with_value_and_returns['frequency'], summary_with_value_and_returns['monetary_value']) 
    
    # Refitting the BG/NBD model with the same data if frequency, recency or T are not zero length vectors
    if not (len(x) == 0 for x in [summary_with_value_and_returns['recency'],summary_with_value_and_returns['frequency'],summary_with_value_and_returns['T']]):
      bgf.fit(summary_with_value_and_returns['frequency'],summary_with_value_and_returns['recency'],summary_with_value_and_returns['T'])
  
    # estimate ltv model
    #[bgf, ggf] = self.estimate_clv_model(query_job.to_dataframe())
    logging.info(bgf)
    
    with gcs.open(self.output_bucket + 'bgf_test.pkl', 'wb') as out_file:
      dill.dump(bgf, out_file)
    with gcs.open(self.output_bucket + 'ggf_test.pkl', 'wb') as out_file:
      dill.dump(ggf, out_file)
    
    #bgf.save_model(self.output_bucket + 'bgf_test.pkl', save_data=False, save_generate_data_method=False)
    #ggf.save_model(self.output_bucket + 'ggf_test.pkl', save_data=False, save_generate_data_method=False)


class CalcLTVFn(beam.DoFn):
  """A DoFn that calculates LTV based on model passed in. """
  def __init__(self, bgf, ggf, ln_exp_max=300, t=14, clv_prediction_time=12, discount_rate=0.01):
    r, alpha, a, b = bgf._unload_params('r', 'alpha', 'a', 'b')
    self.r = r 
    self.a = a
    self.b = b
    self.alpha = alpha
    p, q, v = ggf._unload_params('p', 'q', 'v')
    self.p = p 
    self.q = q
    self.v = v
    
    self.clv_prediction_time = clv_prediction_time # months
    self.discount_rate = discount_rate #0.01 monthly discount rate ~ 12.7% annually)
    self.ln_exp_max = ln_exp_max
    self.t = t


  def conditional_probability_alive(self, frequency, recency, T):
    from numpy import log, where, exp, asscalar
    log_div = ( self.r + frequency ) \
    			* log( (self.alpha + T) / (self.alpha + recency) ) \
    			+ log( self.a / (self.b + where(frequency==0, 1, frequency) -1 ) )
    
    conditional_probability_alive = where( frequency==0, 1, \
    										where( log_div > self.ln_exp_max, 0., \
    												1. / ( 1 + exp( where( log_div > self.ln_exp_max, self.ln_exp_max, log_div) ) ) ) )
    return asscalar(conditional_probability_alive)


  def conditional_expected_average_profit(self, frequency, monetary_value):
    # The expected average profit is a weighted average of individual monetary value and the population mean.
    individual_weight = self.p * frequency / (self.p * frequency + self.q - 1)
    population_mean = self.v * self.p / (self.q - 1)
    return (1 - individual_weight) * population_mean + individual_weight * monetary_value


  def conditional_purchases_up_to_t(self, t_i, frequency, recency, T):
    from numpy import log, isinf, exp
    from scipy.special import hyp2f1
    x = frequency
 
    _a = self.r + frequency
    _b = self.b + frequency
    _c = self.a + self.b + frequency - 1
    _z = t_i / (self.alpha + T + t_i)
    ln_hyp_term = log(hyp2f1(_a, _b, _c, _z))
    # if the value is inf, we are using a different but equivalent formula to compute the function evaluation.
    ln_hyp_term_alt = log(hyp2f1(_c - _a, _c - _b, _c, _z)) + (_c - _a - _b) * log(1 - _z)
    ln_hyp_term = ln_hyp_term_alt if isinf(ln_hyp_term) else ln_hyp_term
    first_term = (self.a + self.b + frequency - 1) / (self.a - 1)
    second_term = (1 - exp(ln_hyp_term + (self.r + frequency) * log((self.alpha + T) / (self.alpha + t_i + T))))

    numerator = first_term * second_term
    denominator = 1 + (frequency > 0) * (self.a / (self.b + frequency - 1)) * ((self.alpha + T) / (self.alpha + recency)) ** (self.r + frequency)
    return numerator / denominator

  
  def customer_lifetime_value(self, frequency, recency, T, monetary_value):
    clv = 0
    for i in range(30, (self.clv_prediction_time * 30) + 1, 30):
      # since the prediction of number of transactions is cumulative, we have to subtract off the previous periods
      expected_number_of_transactions = self.conditional_purchases_up_to_t(i, frequency, recency, T) - self.conditional_purchases_up_to_t(i - 30, frequency, recency, T)
      # sum up the CLV estimates of all of the periods
      clv += (monetary_value * expected_number_of_transactions) / (1 + self.discount_rate) ** (i / 30)
      # Prevent NaN on the pred_clv column
    return 0.0 if clv==0 else clv


  def process(self, element):
    from datetime import date
    
    # element is an immutable dictionary so apply list comprehension to instantiate our return dict
    #ret_dict = { key:val for key, val in element.items() }
    ret_dict = {key.encode('ascii'):val for key, val in element.items()}
    
    #logging.info('Calculating Conditional Probability Alive')
    ret_dict['alive_probability'] = self.conditional_probability_alive(element['frequency'], element['recency'], element['T']) 
    
    #logging.info('Calculating predicted_searches_14_days')
    #summary_df['predicted_searches'] = bgf.conditional_expected_number_of_purchases_up_to_time(t,summary_df['frequency'],summary_df['recency'],summary_df['T'])
    ret_dict['predicted_searches'] = self.conditional_purchases_up_to_t(self.t, element['frequency'], element['recency'], element['T']).item() #cast to native type
    
    #logging.info('Calculating predicted_clv_12_months')
    # use the Gamma-Gamma estimates for the monetary_values
    adjusted_monetary_value = self.conditional_expected_average_profit(element['frequency'], element['monetary_value'])
    ret_dict['predicted_clv_12_months'] = self.customer_lifetime_value(element['frequency'], element['recency'], element['T'], adjusted_monetary_value).item() #cast to native type

    #logging.info('Calculating total_clv')
    # Create column that combines historical and predicted customer value
    #df_final['total_clv'] = df_final['pred_values'] + df_final['historical_clv'] #df_final['Revenue']
    ret_dict['total_clv'] = ret_dict['predicted_clv_12_months'] + ret_dict['historical_clv']

    #logging.info('Calculating days_since_last_active')
    # Create column which calculates in days the number of days since they were last active
    # df_final['last_active'] = df_final['T'] - df_final['recency']
    ret_dict['days_since_last_active'] = element['T'] - element['recency']

    #logging.info('Calculating user_status')
    # Create a column which labels users inactive over 14 days as "Expired" ELSE "Active"
    ret_dict['user_status'] = 'Expired' if ret_dict['days_since_last_active'] > 14 else 'Active'
    ret_dict['user_status'] = ret_dict['user_status'] #.encode('utf-8')
    
    #logging.info('Calculating calc_date')
    ret_dict['calc_date'] = str(date.today()) #.encode('utf-8')
      
    #logging.info('ret_dict: %s', ret_dict)
    #for key, val in ret_dict.items():
    #  logging.info('{} type: {} ;value type: {}'.format(key, type(key), type(val)))
      
    yield ret_dict


def convert_my_dict_to_csv_record(input_dict):
    """ Turns dictionary values into a comma-separated value formatted string """
    return ','.join(map(str, input_dict.values()))
    
def calculate_min_sample_size():
  # model error params
  margin_of_error = .01
  confidence_level = 2.576
  sigma = .5
  min_sample_size = int(ceil(power(confidence_level,2) * sigma*(1-sigma) / power(margin_of_error,2)))
  return min_sample_size
  
  
def estimate_clv_model(summary, model_penalizer=None):

  start = time.clock()

  #set default values if they are not stated
  if model_penalizer is None:
    model_penalizer = 0

  # Building the Model using BG/NBD
  bgf = BetaGeoFitter(penalizer_coef=model_penalizer)
  bgf.fit(summary['frequency'], summary['recency'], summary['T'])

  # There cannot be non-positive values in the monetary_value or frequency vector
  summary_with_value_and_returns = summary[(summary['monetary_value']>0) & (summary['frequency']>0)]
  # Setting up Gamma Gamma model
  ggf = GammaGammaFitter(penalizer_coef = 0)
  ggf.fit(summary_with_value_and_returns['frequency'], summary_with_value_and_returns['monetary_value']) 

  # Refitting the BG/NBD model with the same data if frequency, recency or T are not zero length vectors
  if not (len(x) == 0 for x in [summary_with_value_and_returns['recency'],summary_with_value_and_returns['frequency'],summary_with_value_and_returns['T']]):
    bgf.fit(summary_with_value_and_returns['frequency'],summary_with_value_and_returns['recency'],summary_with_value_and_returns['T'])

  #print('estimate_clv_model runtime: ' + str(time.clock() - start))
  logging.info('estimate_clv_model runtime: ' + str(time.clock() - start))
  
  return [bgf, ggf]
  

def estimate_model(sample_size, output_bucket='gs://ltv-dataflow/tmp/'):
    logging.info('Estimate model on sample size: {}'.format(sample_size))

    qry = ("SELECT client_id, frequency, recency, T, monetary_value FROM ltv.summary ORDER BY RAND() LIMIT {}").format(sample_size)

    query_job = bq_client.query(qry)
    df_random_sample = query_job.to_dataframe() # no need to go through query_job.result()
    #df_random_sample.head(3)
  
    # estimate ltv model
    [bgf, ggf] = estimate_clv_model(df_random_sample)
    logging.info(bgf)
    
    #bgf.save_model(output_bucket + 'bgf_test.pkl', save_data=False, save_generate_data_method=False)
    #ggf.save_model(utput_bucket + 'ggf_test.pkl', save_data=False, save_generate_data_method=False)
    #with gcs.open(path, 'wb') as out_file:
    dill.dump(bgf, gcs.open(output_bucket + 'bgf_test.pkl', 'wb'))
    dill.dump(ggf, gcs.open(output_bucket + 'ggf_test.pkl', 'wb'))
    

def run(argv=None):
  
  """The main function which creates the pipeline and runs it."""
  parser = argparse.ArgumentParser()
  parser.add_argument('--output', dest='output', required=False,
                        help='Output BQ table to write results to.',
  						default='ltv.calc')

  # <lifetimes.BetaGeoFitter: a: 0.30, alpha: 1.70, b: 1.66, r: 0.22>
  # <lifetimes.GammaGammaFitter: p: 0.40, q: 0.80, v: 0.01>
  
  # Parse arguments from the command line.
  known_args, pipeline_args = parser.parse_known_args(argv)

  # load parquet files into bq, overwrite 
  #load_data_bq('ltv', 'summary', 'gs://telemetry-to-gcp/nawong/tmp/summary/*.parquet')
  #load_data_bq('ltv', 'details', 'gs://telemetry-to-gcp/nawong/tmp/details/*.parquet')

  # read in the output bg table schema
  bg_out_schema= ''
  schema_file =  "gs://ltv-dataflow/templates/input/summary_schema.json"
  with gcs.open(schema_file) as f:
  #with beam.Filesystem.open(schema_file) as f:
	data = f.read()
	# Wrapping the schema in fields is required for the BigQuery API.
	bg_out_schema = '{"fields": ' + data + '}'
  #print(bg_out_schema)
  schema = parse_table_schema_from_json(bg_out_schema)
  
  start = time.clock()

#ret_dict: {'user_status': 'Expired', 'historical_clv': 0.013694412532800003, 'total_clv': 0.013694412532811145, 'calc_date': '2019-02-09', 'frequency': 15, 'recency': 64, 'T': 532, 'client_id': u'64ba414c3820805b1f64021cf3e082b091dec4f4', 'historical_searches': 68, 'predicted_searches': 2.969591783030548e-13, 'days_since_last_active': 468, 'predicted_clv_12_months': 1.1142041118488552e-14, 'alive_probability': 7.466582168595789e-13, 'monetary_value': 0.0008995349408800001}
  #(p
  #| 'Read' >> beam.Create( [{'user_status': 'Expired', 'historical_clv': 0.013694412532800003, 'total_clv': 0.013694412532811145, 'calc_date': '2019-02-09', 'frequency': 15, 'recency': 64, 'T': 532, 'client_id': u'64ba414c3820805b1f64021cf3e082b091dec4f4', 'historical_searches': 68, 'predicted_searches': 2.969591783030548e-13, 'days_since_last_active': 468, 'predicted_clv_12_months': 1.1142041118488552e-14, 'alive_probability': 7.466582168595789e-13, 'monetary_value': 0.0008995349408800001}, {'user_status': 'Expired', 'historical_clv': 0.0881827093, 'total_clv': 0.1293670064002583, 'calc_date': '2019-02-09', 'frequency': 1, 'recency': 135, 'T': 642, 'client_id': u'3691929b0e07e22c86c1167c83ded58f481caf89', 'historical_searches': 7, 'predicted_searches': 0.01203839587111248, 'days_since_last_active': 507, 'predicted_clv_12_months': 0.04118429710025831, 'alive_probability': 0.45546193310612315, 'monetary_value': 0.06298764949999999}] )
  #| 'Write to BQ' >> beam.io.Write(beam.io.BigQuerySink(known_args.output, schema=schema,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
  #)
  
  # estimate LTV model
	
  min_sample_size = calculate_min_sample_size()
  #estimate_model(min_sample_size)
  
  pipeline_options = PipelineOptions(pipeline_args)
  #pipeline_options.view_as(SetupOptions).save_main_session = True
  p = beam.Pipeline(options=PipelineOptions(pipeline_args))
               
  dummy = p | 'Read' >> beam.Create( ['dummy'] ) | 'Estimate Lifetimes Model' >> beam.ParDo(EstimateLTVFn(min_sample_size))
  p.run().wait_until_finish() # fun estimation first before moving on to calculation
  
  #reloaded_bgf = dill.load(gcs.open("gs://ltv-dataflow/tmp/bgf_test.pkl", 'rb'))
  #logger.info(reloaded_bgf)


  #data_query = ("SELECT * FROM ltv.summary")
  #data_query = ("SELECT * FROM ltv.summary ORDER BY RAND() LIMIT {}").format(1000000)
  data_query = ("SELECT * FROM ltv.summary where client_id in ('3691929b0e07e22c86c1167c83ded58f481caf89','64ba414c3820805b1f64021cf3e082b091dec4f4','3c30753419c6346d975d99d3e93b8f500c2b5bb3')")
  #data_query = ("SELECT summ.*, det.* EXCEPT (client_id) FROM ltv.summary summ LEFT JOIN ltv.details det ON summ.client_id=det.client_id WHERE summ.client_id in ('3691929b0e07e22c86c1167c83ded58f481caf89','64ba414c3820805b1f64021cf3e082b091dec4f4')")
  (p
  #| 'Read' >> beam.Create( [{'dummy': 'dummy'}] )
  #| 'Estimate Lifetimes Model' >> beam.ParDo(EstimateLTVFn(calculate_min_sample_size()))
  #| 'Wait on Estimation' >> Wait.On(dummy)
  | 'Read Orders from BigQuery ' >> beam.io.Read(beam.io.BigQuerySource(query=data_query, use_standard_sql=True))
  | 'Apply Lifetimes Model' >> beam.ParDo( CalcLTVFn( dill.load(gcs.open("gs://ltv-dataflow/tmp/bgf_test.pkl", 'rb')), dill.load(gcs.open("gs://ltv-dataflow/tmp/ggf_test.pkl", 'rb')) ) )
  #| beam.io.WriteToText('gs://ltv-dataflow/output/ltv_output.txt', file_name_suffix='.csv')
  #| 'Write Data to Parquet' >> beam.io.WriteToParquet('gs://ltv-dataflow/output/',pyarrow.schema([('name', pyarrow.binary()), ('age', pyarrow.int64())]))
  | 'Write Data to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(known_args.output, schema=schema,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
  )
  p.run().wait_until_finish()
  logging.info('beam_calc runtime: ' + str(time.clock() - start))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  #run_beam()
  run()
