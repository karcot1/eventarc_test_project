# Copyright 2023 Google, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START eventarc_gcs_server]
from __future__ import annotations

from typing import Any
import os
import sys

import google.auth
from google.auth.transport.requests import AuthorizedSession
import requests

from flask import Flask, request
import json
from google.cloud import bigquery


app = Flask(__name__)
# [END eventarc_gcs_server]

AUTH_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])

# [START eventarc_gcs_handler]
@app.route('/', methods=['POST'])
def index():
    
    # Log entry: insert statement detected
    entry = dict(severity="NOTICE", message="Insert statement detected - running initial checks", component="cloud-run-job-start" )
    print(json.dumps(entry))

    # Gets the Payload data from the Audit Log
    content = request.json
    try:

        # Log entry: checking metadata
        entry = dict( severity="NOTICE", message="Checking metadata...", component="get-logging-metadata" )
        print(json.dumps(entry))

        ds = content['resource']['labels']['dataset_id']
        tbl = content['protoPayload']['resourceName']
        rows = int(content['protoPayload']['metadata']['tableDataChange']['insertedRowsCount'])
        
        if ds == 'dataform' and tbl.endswith('tables/ad_and_ingest_metadata') and rows > 0:
            # Metadata passes
            entry = dict( severity="NOTICE", message="Metadata successfully meets criteria. Running assessment...", component="run-assessment" )
            print(json.dumps(entry))

            # Run assessment on metadata object in BigQuery
            assessment = assess_ingest_tables()
            
            #Identify DAG to trigger using rules engine
            entry = dict( severity="NOTICE", message="Assessment completed. Checking Rules Engine...", component="check-rules" )
            print(json.dumps(entry))

            dag_to_trigger = rules_engine(assessment)
            if dag_to_trigger:

                # Trigger DAG
                entry = dict( severity="NOTICE", message="Rules applied. Triggering {}...".format(dag_to_trigger), component="trigger-dag" )
                print(json.dumps(entry))

                #TODO: Trigger DAG
                return "DAG successfully triggered", 200
            else:
                entry = dict( severity="NOTICE", message="Assessment completed. No DAG to trigger.", component="no-dag" )
                print(json.dumps(entry))
                return "no DAG to trigger", 200
    except:
        # if these fields are not in the JSON, ignore
        entry = dict( severity="NOTICE", message="Metadata does not meet criteria. Stopping...", component="wrong-log" )
        print(json.dumps(entry))
        pass
    entry = dict( severity="NOTICE", message="DONE.", component="done" )
    print(json.dumps(entry))
    return "ok", 200
# [END eventarc_gcs_handler]

def assess_ingest_tables():

    entry = dict( severity="NOTICE", message="Running query to pull ingestion table data...", component="pull-ingest-data" )
    print(json.dumps(entry))

    client = bigquery.Client()
    query = """
SELECT
    INGEST_CD,
    OBJECT,
    STATUS,
    LOAD_DATE
FROM dataform.ad_and_ingest_metadata
WHERE STATUS = "SUCCESS" AND EXTRACT(DATE FROM LOAD_DATE) = EXTRACT(DATE FROM CURRENT_TIMESTAMP())
    """
    try:
        results = client.query(query)
        assessment = [[row[i] for row in list(results)] for i in range(len(list(results)[0]))][1]
        print(assessment)
    except Exception as e:
        entry = dict( severity="ERROR", message="Unable to execute BigQuery Job: {}".format(e), component="assess-ingest-tables" )
        print(json.dumps(entry))
    return assessment
 

def rules_engine(assessment):
    client = bigquery.Client()
    print('Attempting to open rules file...')
    try: 
        f = open('rules.json')
        rules = json.load(f)
        print(rules)

    except Exception as e:
        entry = dict( severity="ERROR", message="Unable to read JSON file: {}".format(e), component="rules-engine" )
        print(json.dumps(entry))

    dag_to_invoke = 'None'
    
    entry = dict( severity="NOTICE", message="Applying rules...", component="apply-rules" )
    print(json.dumps(entry))

    for analytical_domain, dependencies in rules.items():
        if set(dependencies).issubset(set(assessment)) and analytical_domain not in assessment:
            dag_to_invoke = analytical_domain
            break
    # Insert to dag_invocation table to log triggering of AD build
    query = f"""
INSERT INTO dataform.dag_invocations
VALUES('{dag_to_invoke}', CURRENT_TIMESTAMP())
    """

    print("Running query: ", query)

    try:
        client.query(query)
    except Exception as e:
        entry = dict( severity="ERROR", message="Unable to execute BigQuery job: {}".format(e), component="rules-engine" )
        print(json.dumps(entry))

    return dag_to_invoke

# [START eventarc_gcs_server]
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
# [END eventarc_gcs_server]
