#!/bin/bash

SERVICE=bq-cloud-run
PROJECT=$(gcloud config get-value project)
CONTAINER="gcr.io/${PROJECT}/${SERVICE}"
REGION="us-central1"
PROJECT_NO=$(gcloud projects list --filter="$PROJECT" --format="value(PROJECT_NUMBER)")
SVC_ACCOUNT="${PROJECT_NO}-compute@developer.gserviceaccount.com"

# Build docker image and deploy it to Cloud Run (Not needed if connecting the service via GitHub)
gcloud builds submit --tag ${CONTAINER}
gcloud run deploy ${SERVICE} --image $CONTAINER --platform managed

# exit;

# Setup authentication
gcloud config set run/region $REGION
gcloud projects add-iam-policy-binding $PROJECT \
    --member="serviceAccount:service-${PROJECT_NO}@gcp-sa-pubsub.iam.gserviceaccount.com"\
    --role='roles/iam.serviceAccountTokenCreator'

gcloud projects add-iam-policy-binding $PROJECT \
    --member=serviceAccount:${SVC_ACCOUNT} \
    --role='roles/eventarc.admin'

# Create a trigger from BigQuery
gcloud eventarc triggers delete bq-cloud-run-trigger --location us-central1
gcloud eventarc triggers create bq-cloud-run-trigger \
  --location us-central1 --service-account "1054873895331-compute@developer.gserviceaccount.com" \
  --destination-run-service bq-cloud-run  \
  --event-filters type=google.cloud.audit.log.v1.written \
  --event-filters methodName=google.cloud.bigquery.v2.JobService.InsertJob \
  --event-filters serviceName=bigquery.googleapis.com \
  --event-filters resourceName=projects/dataform-test-362521/datasets/dataform/tables/ad_and_ingest_metadata
  
#  --event-filters resourceName=projects/_/buckets/"$MY_GCS_BUCKET"
