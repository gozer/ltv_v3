![Mozilla logo](https://github.com/ophie200/ltv_v3/blob/master/images/Mozilla-Logo-300x124.png)

# LTV on GCP

Scripts to set up LTV on GCP.

## Installing / Getting started

To start running LTV on your local machine:

Set up your local drive with virtualenv using python 2.7 (latest version of Dataflow Python SDK)
```shell
git clone https://github.com/ophie200/ltv_v3.git
cd ltv_v3/
pip install apache-beam[gcp]
export GOOGLE_APPLICATION_CREDENTIALS = <your json api key file here>
```

Run Dataflow job locally using DirectRunner:
```shell
python ltv_beam.py --project imposing-union-227917 --temp_location gs://ltv-dataflow/tmp --staging_location gs://ltv-dataflow/staging
```
This will run the job using your local machine CPU/memory to process LTV data on GCP. 

Create & stage template on GCP:
```shell
python ltv_beam.py --runner DataflowRunner --project imposing-union-227917 --staging_location gs://ltv-dataflow/staging --temp_location gs://ltv-dataflow/tmp --template_location gs://ltv-dataflow/templates/ltv-dataflow-template --requirements_file requirements.txt
```
This packages the Dataflow pipeline defined in ltv_beam.py into an executable and add any required libraries to the staging_location. The template job references the packaged pipeline and is defined in template_location.


Execute template on dataflow as batch job:
```shell
gcloud dataflow jobs run run-ltv-dataflow-template --gcs-location gs://ltv-dataflow/templates/ltv-dataflow-template
```
This runs the template. You can view the progress from the Dataflow UI.


## Licensing
Licensed under ... For details, see the LICENSE file.
