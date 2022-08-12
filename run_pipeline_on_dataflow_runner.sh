#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

source _dataflow_utils.sh

if [ ! -f '.env' ]; then
  echo "File .env  not found. You must create it. To do it, you can use .env.example";
  exit 1;
fi

# Use dockerize java and Apache Maven
alias mvn="./mvnw"
alias java="./javaw"

export $(cat .env | grep -v "^#" | xargs)
JAR_FILE="target/ingest-pubsub-to-snowflake-bundled-1.0.jar"
if [ ! -f "${JAR_FILE}" ]; then
# [START compile_dataflow_runner]
mvn package -P "dataflow-runner" --batch-mode
# [END]
fi

SNOWFLAKE_PRIVATE_KEY=$(cat rsa_key.p8 | tail -n +2 | tail -r | tail -n +2 | tail -r)
set -x

GCP_PROJECT_ID="$(gcloud config get-value core/project)"
PIPELINE_PUBSUB_SUBSCRIPTION_FQN="projects/${GCP_PROJECT_ID}/subscriptions/${PIPELINE_PUBSUB_SUBSCRIPTION}"

DATAFLOW_APP_NAME="ingestpubsubtosnowflake${RANDOM}dataflow"

# [START run_dataflow_runner]
java -jar target/ingest-pubsub-to-snowflake-bundled-1.0.jar \
   --runner=DataflowRunner \
   --project="${GCP_PROJECT_ID}" \
   --region="${DATAFLOW_REGION}" \
   --appName="${DATAFLOW_APP_NAME}" \
   --serverName="${SNOWFLAKE_SERVER_NAME}" \
   --username="${SNOWFLAKE_USERNAME}" \
   --rawPrivateKey="${SNOWFLAKE_PRIVATE_KEY}" \
   --privateKeyPassphrase="${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}" \
   --database="${SNOWFLAKE_DATABASE}" \
   --schema="${SNOWFLAKE_SCHEMA}" \
   --role="${SNOWFLAKE_ROLE}" \
   --storageIntegrationName="${SNOWFLAKE_STORAGE_INTEGRATION}" \
   --inputSubscription="${PIPELINE_PUBSUB_SUBSCRIPTION_FQN}" \
   --snowPipe="${SNOWFLAKE_PIPE}" \
   --outputTable="${PIPELINE_SNOWFLAKE_OUTPUT_TABLE}" \
   --gcpTempLocation="gs://${DATAFLOW_BUCKET}/temp" \
   --stagingBucketName="gs://${DATAFLOW_BUCKET}/staging"
# [END]

wait_for_job_start "${DATAFLOW_APP_NAME}" "name ~ ${DATAFLOW_APP_NAME}"
JOB_ID=$(gcloud dataflow jobs list --region="${DATAFLOW_REGION}" --format 'get(id)' --filter="name ~ ${DATAFLOW_APP_NAME}")
current_state=$(gcloud dataflow jobs describe "${JOB_ID}" --region="${DATAFLOW_REGION}" --format 'get(currentState)')
echo "Job running. Job ID: ${JOB_ID}, current state: ${current_state}"

trap "cancel_dataflow_job "$JOB_ID"" SIGINT EXIT

while : ; do
  echo "Sending messages. Press CTRL+C to cancel job/"
  sleep 5;
done