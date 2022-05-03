#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

source _dataflow_utils.sh

if [ ! -f .env ]; then
  echo "File .env  not found. You must create it. To do it, you can use .env.example";
  exit 1;
fi
export $(cat .env | grep -v "^#" | xargs)

GCP_PROJECT_ID="$(gcloud config get-value core/project)"
PIPELINE_PUBSUB_TOPIC_FQN="projects/${GCP_PROJECT_ID}/topics/${PIPELINE_PUBSUB_TOPIC}"
PIPELINE_PUBSUB_SUBSCRIPTION_FQN="projects/${GCP_PROJECT_ID}/subscriptions/${PIPELINE_PUBSUB_SUBSCRIPTION}"

GENERATOR_JOB_NAME="streaming-data-generator-$RANDOM"
echo "Submitting jobs"
gcloud dataflow flex-template run "${GENERATOR_JOB_NAME}" \
   --project="${GCP_PROJECT_ID}" \
   --region="${DATAFLOW_REGION}" \
   --template-file-gcs-location=gs://dataflow-templates/latest/flex/Streaming_Data_Generator \
   --parameters \
schemaLocation="gs://${DATAFLOW_BUCKET}/stream-schema.json",\
qps=1,\
topic="${PIPELINE_PUBSUB_TOPIC_FQN}"

sleep 3
echo "Waiting for job start"
wait_for_job_start "${GENERATOR_JOB_NAME}" "name = ${GENERATOR_JOB_NAME}"
JOB_ID=$(gcloud dataflow jobs list --region="${DATAFLOW_REGION}" --format 'get(id)' --filter="name = ${GENERATOR_JOB_NAME}")
current_state=$(gcloud dataflow jobs describe "${JOB_ID}" --region="${DATAFLOW_REGION}" --format 'get(currentState)')
echo "Job running. Job ID: ${JOB_ID}, current state: ${current_state}"

trap "cancel_dataflow_job "$JOB_ID"" SIGINT

while : ; do
  echo "Sending messages. Press CTRL+C to cancel job/"
  sleep 5;
done