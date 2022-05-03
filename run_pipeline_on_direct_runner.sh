#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

if [ ! -f '.env' ]; then
  echo "File .env  not found. You must create it. To do it, you can use .env.example";
  exit 1;
fi

# Use dockerize java and Apache Maven
alias mvn="./mvnw"
alias java="./javaw"

export $(cat .env | grep -v "^#" | xargs)

# Use dockerize java and Apache Maven
alias mvn="./mvnw"
alias java="./javaw"

JAR_FILE="target/ingest-pubsub-to-snowflake-bundled-1.0.jar"
if [ ! -f "${JAR_FILE}" ]; then
# [START compile_direct_runner]
mvn package -P "direct-runner" --batch-mode
# [END]
fi

SNOWFLAKE_PRIVATE_KEY=$(cat rsa_key.p8 | tail -n +2 | tail -r | tail -n +2 | tail -r)
set -x

GCP_PROJECT_ID="$(gcloud config get-value core/project)"
PIPELINE_PUBSUB_SUBSCRIPTION_FQN="projects/${GCP_PROJECT_ID}/subscriptions/${PIPELINE_PUBSUB_SUBSCRIPTION}"

DATAFLOW_APP_NAME="ingestpubsubtosnowflake$RANDOM"

trap "pkill -f 'java -jar target/ingest-pubsub-to-snowflake-bundled-1.0.jar'" SIGINT EXIT

# [START run_direct_runner]
java -jar target/ingest-pubsub-to-snowflake-bundled-1.0.jar \
   --runner=DirectRunner \
   --serverName="${SNOWFLAKE_SERVER_NAME}" \
   --username="${SNOWFLAKE_USERNAME}" \
   --database="${SNOWFLAKE_DATABASE}" \
   --schema="${SNOWFLAKE_SCHEMA}" \
   --role="${SNOWFLAKE_ROLE}" \
   --rawPrivateKey="${SNOWFLAKE_PRIVATE_KEY}" \
   --snowPipe="${SNOWFLAKE_PIPE}" \
   --privateKeyPassphrase="${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}" \
   --storageIntegrationName="${SNOWFLAKE_STORAGE_INTEGRATION}" \
   --inputSubscription="${PIPELINE_PUBSUB_SUBSCRIPTION_FQN}" \
   --outputTable="${PIPELINE_SNOWFLAKE_OUTPUT_TABLE}" \
   --gcpTempLocation="gs://${DATAFLOW_BUCKET}/temp" \
   --tempLocation="gs://${DATAFLOW_BUCKET}/temp" \
   --stagingBucketName="gs://${DATAFLOW_BUCKET}/staging"
# [END]