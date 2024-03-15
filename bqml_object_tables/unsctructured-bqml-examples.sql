/* 
Copyright 2023 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/


/* 
This is a sample sql script that runs through several bigquery object table examples leveraging BQML to analyze unscructured data
*/

-- 1. Create empty dataset for object table
create schema `<REPLACE WITH PROJECT>.unstruct_data_example`;

-- 2. Identify GCS buckets, Create Connection
/*
1. Create a new google cloud storage bucket
2. Upload example photos from repository, keep the names of the buckets the same as they are reference later in the object table creation
3. Create a new Connection
Click Add in the left nav window -> Click connections to external data sources -> Click  Vertex AI remote models, remote functions and BigLake (Cloud Resource) -> Name the connection 'unstruct-data-conn' 
*more information on creating a connection: https://cloud.google.com/bigquery/docs/create-cloud-resource-connection

Copy the service account created for the new connection and grant the following permissions:
- Cloud Functions Invoker
- Cloud Run Invoker
- Document AI API User
- Grant access to the GCS bucket to the service account
*/

-- 3. Create External Table (Landmarks example)
create or replace external table `unstruct_data_example.famous-landmarks`
with connection `us.unstruct-data-conn`
options(
  object_metadata="SIMPLE",
  uris=["gs://bqml-examples/landmark_detection/*"]
);

-- 4. Query newly create object table
select data,* from `dataform-test-362521.unstruct_data.famous-landmarks`;

-- 5. create Vision AI remote model
CREATE OR REPLACE MODEL `dataform-test-362521.unstruct_data.bqml-cloud-ai-vision`
REMOTE WITH CONNECTION `dataform-test-362521.us.unstruct-data-conn`
  OPTIONS ( remote_service_type = 'cloud_ai_vision_v1' );

-- 6. Annotate image
SELECT
string(ml_annotate_image_result["landmark_annotations"][0]["description"]) as landmark,
*
FROM ML.ANNOTATE_IMAGE(
  MODEL `dataform-test-362521.unstruct_data.bqml-cloud-ai-vision`,
  TABLE `dataform-test-362521.unstruct_data.famous-landmarks`,
  STRUCT(['landmark_detection'] AS vision_features)
);

-- 1. Create new object table (faces example)
create or replace external table `unstruct_data.faces`
with connection `us.unstruct-data-conn`
options(
  object_metadata="SIMPLE",
  uris=["gs://bqml-examples/faces/*"]
);

-- 2. Use the same vision AI model used in the landmarks query
SELECT
COALESCE(ARRAY_LENGTH(JSON_EXTRACT_ARRAY(ml_annotate_image_result,'$.face_annotations')),0) count_faces,
*
FROM ML.ANNOTATE_IMAGE(
  MODEL `dataform-test-362521.unstruct_data.bqml-cloud-ai-vision`,
  TABLE `dataform-test-362521.unstruct_data.faces`,
  STRUCT(['face_detection'] AS vision_features)
);


-- 1. Create new object table (image to text example)
create or replace external table `unstruct_data.image-text`
with connection `us.unstruct-data-conn`
options(
  object_metadata="SIMPLE",
  uris=["gs://bqml-examples/image_to_text/*"]
);

-- 2. Use the same vision AI model used in the landmarks query
SELECT
*
FROM ML.ANNOTATE_IMAGE(
  MODEL `dataform-test-362521.unstruct_data.bqml-cloud-ai-vision`,
  TABLE `dataform-test-362521.unstruct_data.image-text`,
  STRUCT(['text_detection'] AS vision_features)
);

-- 1. Create new object table (driver license example)
create or replace external table `unstruct_data.drivers-license`
with connection `us.unstruct-data-conn`
options(
  object_metadata="SIMPLE",
  uris=["gs://bqml-examples/drivers_license_parser/*"]
);

-- 2. create a new document US Driver License Document Processor and copy the ID
-- https://pantheon.corp.google.com/ai/document-ai/processor-library?
-- 3. Create new Doc AI model
CREATE OR REPLACE MODEL `dataform-test-362521.unstruct_data.bqml-cloud-ai-doc`
REMOTE WITH CONNECTION `dataform-test-362521.us.unstruct-data-conn`
  OPTIONS ( 
    remote_service_type = 'CLOUD_AI_DOCUMENT_V1',
    DOCUMENT_PROCESSOR = '<PASTE PROCESSOR ID HERE>'
    -- example format: 'projects/1054873895331/locations/us/processors/f6aff9594b051fe7/processorVersions/pretrained-us-driver-license-v1.0-2021-06-14'
  );

-- 4. Use the Doc AI Model to parse Driver's License info
SELECT
*
FROM ML.PROCESS_DOCUMENT(
  MODEL `dataform-test-362521.unstruct_data.bqml-cloud-ai-doc`,
  TABLE `unstruct_data.drivers-license`
);

-- 1. Create new object table to hold patents (Gemini Pro Example)
create or replace external table `unstruct_data.patents`
with connection `us.unstruct-data-conn`
options(
  object_metadata="SIMPLE",
  uris=["gs://bqml-examples/patents/*"]
);

-- 2. Create Google Cloud Function using the python file called gemini-pro-patent.py and copy the endpoint

-- 3. Create a BQ function with endpoint pointing to GCF
CREATE OR REPLACE FUNCTION `dataform-test-362521.unstruct_data.analyze_patent`(gcs_uri STRING) RETURNS STRING
  REMOTE WITH CONNECTION `dataform-test-362521.us.unstruct-data-conn`
  OPTIONS (
    endpoint = '<PASTE ENDPOINT HERE>',
    max_batching_rows = 1
  );

-- 4. Call function, pass in uris from object table
SELECT
  uri AS image_input,
  `dataform-test-362521.unstruct_data.analyze_patent`(uri) AS image_description
FROM
  `unstruct_data.patents`;

