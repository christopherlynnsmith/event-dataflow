import os
import json
import google.auth
from googleapiclient.discovery import build
from datetime import datetime

def hello_gcs(event,context):
    '''Triggered by a change to a Cloud Storage bucket.
    Args:
          event (dict) : Event payload.request
          context (google.cloud.functions.Context) : Metadata for the event.
    '''

     template="gs://dataflow-testbucket-csmith/templates/beamcloudstorageSQL"

     now = datetime.now()

     s2 = now.strftime("%Y-%m-%d-%H-%M-%S")

     #print("s2:",s2)

     job = "pythonInvokeDataFlowBatch" +s2


        username = os.getenv("DB_USERNAME")
        secret = os.getenv("DB_PASSWORD")

     parameters = {
     "dataPath": "gs://dataflow-testbucket-csmith/data/data-*.txt",
     "jdbcDriver": "com.mysql.jbdc.Driver",
     "jdbcUrl": "jdbc:mysql://10.128.0.93:3307/test",
     "username": username,
     "password": secret
      }


      ''' Script to run Dataflow template. '''
      credentials, project_id = google.auth.default()
      print(project_id)

      dataflow = build('dataflow',  'v1b3')
      request = dataflow.projects().templates().launch(
           projectId=project_id,
           gcsPath=template,
            body={
                "jobName": job,
                "parameters": parameters
            }
      )

      response = request.execute()

      #file = event
      #print(f"Processing file: {file['name']}.")
