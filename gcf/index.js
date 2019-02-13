var {google} = require('googleapis');

exports.triggerDataFlowLTV = (event, callback) => {

    const file = event.data;
    const context = event.context;

	console.log(`  Bucket: ${file.bucket}`);

    //if (file.resourceState === 'exists' && file.name) {
    
		console.log(`  File: ${file.name}`);
    	
        google.auth.getApplicationDefault(function(err, authClient, projectId) {
            if (err) {
                throw err;
            }
            
            console.log(projectId);
            
            const dataflow = google.dataflow({version: 'v1b3', auth: authClient});
            
            console.log(`gs://${file.bucket}/${file.name}`);

            dataflow.projects.templates.create({
            	gcsPath: 'gs://ltv-dataflow/templates/ltv-dataflow-template',
                projectId: projectId,
                resource: {
                    //parameters: {
                    //    input: `gs://${file.bucket}/${file.name}`,
                    //    output: 'gs://ltv-dataflow/output/result.txt'
                    //},
                    jobName: 'gcf-run-ltv-dataflow-template',
                }
            }, function(err, response) {
                if (err) {
                    console.error("problem running dataflow template, error was: ", err);
                }
                console.log("Dataflow template response: ", response);
                callback();
            });
            
        }); //end google.auth.getApplicationDefault

    //}
    //callback();
}; //end exports.goWithTheDataFlowLocal
