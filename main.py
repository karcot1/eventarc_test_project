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
import os
import sys

from flask import Flask, request
import json
from google.cloud import bigquery

app = Flask(__name__)
# [END eventarc_gcs_server]


# [START eventarc_gcs_handler]
@app.route('/', methods=['POST'])
def index():

    sys.stdout.write('Insert statement detected - running initial checks')

    # Gets the Payload data from the Audit Log
    content = request.json
    try:

        sys.stdout.write('Checking metadata...')

        ds = content['resource']['labels']['dataset_id']
        tbl = content['protoPayload']['resourceName']
        rows = int(content['protoPayload']['metadata']['tableDataChange']['insertedRowsCount'])
        if ds == 'dataform' and tbl.endswith('tables/ad_and_ingest_metadata') and rows > 0:
            # Run assessment on metadata object in BigQuery

            sys.stdout.write('Metadata successfully meets criteria. Running assessment...')

            assessment = assess_ingest_tables()
            # Identify DAG to trigger using rules engine
            
            sys.stdout.write('Assessment completed. Checking Rules Engine...')

            dag_to_trigger = rules_engine(assessment)
            if dag_to_trigger:
                sys.stdout.write('Assessment complete - triggering dag')
                return "assessment complete - triggering dag", 200
            else:
                sys.stdout.write('No DAG to trigger')
                return "no DAG to trigger", 200
    except:
        # if these fields are not in the JSON, ignore
        sys.stdout.write('Metadata does not meet criteria. Stopping...')
        pass
    return "ok", 200
# [END eventarc_gcs_handler]

def assess_ingest_tables():
    sys.stdout.write('Running query to pull ingestion table data...')

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
    results = client.query(query)
    return results
 

def rules_engine(query_results):
    client = bigquery.Client()
    # initialize a record
    sys.stdout.write('Initializing current data...')
    src_table_1 = False
    src_table_2 = False
    src_table_3 = False
    src_table_4 = False
    src_table_5 = False
    src_table_6 = False

    retail_account = False
    customer = False
    sales = False

    # create assessment
    sys.stdout.write('Applying rules...')
    for row in query_results:
        if row[1] == 'src_table_1' and row[2] == 'SUCCESS':
            src_table_1 = True
        if row[1] == 'src_table_2' and row[2] == 'SUCCESS':
            src_table_2 = True
        if row[1] == 'src_table_3' and row[2] == 'SUCCESS':
            src_table_3 = True
        if row[1] == 'src_table_4' and row[2] == 'SUCCESS':
            src_table_4 = True
        if row[1] == 'src_table_5' and row[2] == 'SUCCESS':
            src_table_5 = True
        if row[1] == 'src_table_6' and row[2] == 'SUCCESS':
            src_table_6 = True
        if row[1] == 'retail_account' and row[2] == 'SUCCESS':
            retail_account = True
        if row[1] == 'customer' and row[2] == 'SUCCESS':
            customer = True
        if row[1] == 'sales' and row[2] == 'SUCCESS':
            sales = True

    # trigger DAG from rules:
    sys.stdout.write('Selecting DAG to trigger...')
    # 1. retail account - depends on:
    #   src_table_1
    #   src_table_2
    #   src_table_3
    if src_table_1 and src_table_2 and src_table_3 and not retail_account:
        query = """
        INSERT INTO dataform.dag_invocations
        VALUES('retail_account', CURRENT_TIMESTAMP())
            """
    # 2. customer - depends on:
    #   retail_account
    #   src_table_4
    #   src_table_5
    elif retail_account and src_table_4 and src_table_5 and not customer:
        query = """
        INSERT INTO dataform.dag_invocations
        VALUES('customer', CURRENT_TIMESTAMP())
            """
    # 3. sales - depends on:
    #   retail_account
    #   customer
    #   src_table_6
    elif retail_account and customer and src_table_6 and not sales:
        query = """
        INSERT INTO dataform.dag_invocations
        VALUES('sales', CURRENT_TIMESTAMP())
            """
    else:
        query = """
        INSERT INTO dataform.dag_invocations
        VALUES('none', CURRENT_TIMESTAMP())
            """
    client.query(query)
    return query

# [START eventarc_gcs_server]
if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
# [END eventarc_gcs_server]
