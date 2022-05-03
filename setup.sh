#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

if [ ! -f '.env' ]; then
  echo "File .env  not found. You must create it. To do it, you can use .env.example";
  exit 1;
fi

export $(cat .env | grep -v "^#" | xargs)

set -x
## Create a Service User in Snowfalke
# [START snowsql]
snowsql -c "${SNOWSQL_CONN}" -q "SELECT 1"
# [END]

# [START gen_private_key]
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -v1 PBE-SHA1-RC4-128 -out rsa_key.p8 -passout "pass:${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}"
# [END]
# [START set_private_key]
SNOWFLAKE_PRIVATE_KEY=$(cat rsa_key.p8 | tail -n +2 | tail -r | tail -n +2 | tail -r)
# [END]

# [START gen_public_key]
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub -passin "pass:${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}"
# [END]
# [START set_public_key]
SNOWFLAKE_PUB_KEY=$(cat rsa_key.pub | tail -n +2 | tail -r | tail -n +2 | tail -r)
# [END]

# [START verify_private_key]
echo  "It is a secret" > secret.txt
openssl dgst -sha256 -sign rsa_key.p8 -passin "pass:${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}" -out secret.txt.sign secret.txt
openssl dgst -sha256 -verify rsa_key.pub -signature secret.txt.sign secret.txt
rm secret.txt secret.txt.sign
# [END]

snowsql -c echo=true -c "${SNOWSQL_CONN}" -q "GRANT OWNERSHIP ON PIPE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_PIPE} TO ROLE ACCOUNTADMIN REVOKE CURRENT GRANTS" || true

# [START create_user]
snowsql -c "${SNOWSQL_CONN}" -q "
  CREATE OR REPLACE ROLE ${SNOWFLAKE_ROLE};
  CREATE OR REPLACE USER ${SNOWFLAKE_USERNAME} DEFAULT_ROLE=${SNOWFLAKE_ROLE}, DEFAULT_WAREHOUSE=${SNOWFLAKE_WAREHOUSE} RSA_PUBLIC_KEY='${SNOWFLAKE_PUB_KEY}';

  GRANT ROLE ${SNOWFLAKE_ROLE} TO USER ${SNOWFLAKE_USERNAME}
"
# [END]

# [START verify_user]
SNOWSQL_PRIVATE_KEY_PASSPHRASE="${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}" \
   snowsql \
   --accountname "$(echo "${SNOWFLAKE_SERVER_NAME}" | cut -d "." -f 1-2)" \
   --username "${SNOWFLAKE_USERNAME}" \
   --dbname "${SNOWFLAKE_DATABASE}" \
   --schemaname "${SNOWFLAKE_SCHEMA}" \
   --warehouse "${SNOWFLAKE_WAREHOUSE}" \
   --rolename "${SNOWFLAKE_ROLE}" \
   --private-key-path "rsa_key.p8" \
   --query 'SELECT CURRENT_ROLE(), CURRENT_USER()';
# [END]

## Setting up database, schema in Snowflake

# [START create_database]
snowsql -c "${SNOWSQL_CONN}" -q "
  CREATE OR REPLACE DATABASE ${SNOWFLAKE_DATABASE};
  CREATE OR REPLACE SCHEMA ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA};

  GRANT USAGE ON DATABASE ${SNOWFLAKE_DATABASE} TO ROLE ${SNOWFLAKE_ROLE};
  GRANT USAGE ON SCHEMA ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA} TO ROLE ${SNOWFLAKE_ROLE};
"
# [END]

# [START verify_database]
SNOWSQL_PRIVATE_KEY_PASSPHRASE="${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}" \
   snowsql \
   --accountname "$(echo "${SNOWFLAKE_SERVER_NAME}" | cut -d "." -f 1-2)" \
   --username "${SNOWFLAKE_USERNAME}" \
   --private-key-path "rsa_key.p8" \
   --query 'SELECT CURRENT_ROLE(), CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA()';
# [END]

## Setting up a bucket in GCP, stage and tables in Snowflake
if ! gsutil ls "gs://${DATAFLOW_BUCKET}"; then
# [START create_bucket]
gsutil mb -c standard "gs://${DATAFLOW_BUCKET}"
# [END]
fi

# [START create_integration]
snowsql -c "${SNOWSQL_CONN}" -q "
  CREATE OR REPLACE STORAGE INTEGRATION ${SNOWFLAKE_STORAGE_INTEGRATION}
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://${DATAFLOW_BUCKET}/');
"
# [END]

# [START assign_bucket]
SNOWFLAKE_STORAGE_INTEGRATION_SA_EMAIL=$(snowsql -c "${SNOWSQL_CONN}" -q "DESC STORAGE INTEGRATION ${SNOWFLAKE_STORAGE_INTEGRATION};" -o output_format=json -o friendly=false -o timing=false | jq '.[] | select(.property == "STORAGE_GCP_SERVICE_ACCOUNT") | .property_value' -r)
   gsutil iam ch "serviceAccount:${SNOWFLAKE_STORAGE_INTEGRATION_SA_EMAIL}:roles/storage.admin" "gs://${DATAFLOW_BUCKET}"
# [END]

# [START create_stage]
snowsql -c "${SNOWSQL_CONN}" -q "
  CREATE OR REPLACE STAGE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE}
  URL='gcs://${DATAFLOW_BUCKET}/staging'
  STORAGE_INTEGRATION = ${SNOWFLAKE_STORAGE_INTEGRATION};

  GRANT USAGE ON STAGE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE} TO ROLE ${SNOWFLAKE_ROLE};
"
# [END]

# [START create_table]
snowsql -c "${SNOWSQL_CONN}" -q "
  CREATE OR REPLACE TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE} (id TEXT, name TEXT, age INTEGER, price FLOAT);

  GRANT INSERT ON TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE} TO ROLE ${SNOWFLAKE_ROLE};
  GRANT SELECT ON TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE} TO ROLE ${SNOWFLAKE_ROLE};
"
# [END]

# [START verify_load]
FILENAME=test-data-${RANDOM}.csv.gz
echo "'16f0a88b-af94-4707-9f91-c1dd125f271c','A blue door',48,12.5
'df9efd67-67d6-487d-9ad4-92537cf25eaa','A yellow door',16,12.5
'04585e7f-f340-4d2e-8371-ffbc162c4354','A pink door',26,12.5
'd52275c0-d6c6-4331-8248-784255bef654','A purple door',13,12.5" | gzip | gsutil cp - "gs://${DATAFLOW_BUCKET}/staging/${FILENAME}"
snowsql -c "${SNOWSQL_CONN}" -q "
  COPY INTO ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE} FROM @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE}/${FILENAME};
"
# [END]

# [START display_table]
snowsql -c "${SNOWSQL_CONN}" -q "
  SELECT * FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE} LIMIT 4
"
# [END]

# [START truncate_table]
snowsql -c "${SNOWSQL_CONN}" -q "
  TRUNCATE TABLE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE}
"
# [END]

# [START create_pipe]
snowsql -c "${SNOWSQL_CONN}" -q "
  CREATE OR REPLACE PIPE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_PIPE} AS
  COPY INTO ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE} FROM @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_STAGE};
  ALTER PIPE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_PIPE} SET PIPE_EXECUTION_PAUSED=true;

  GRANT OWNERSHIP ON PIPE ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_PIPE} TO ROLE ${SNOWFLAKE_ROLE};
"
# [END]

# [START resume_pipe]
SNOWSQL_PRIVATE_KEY_PASSPHRASE="${SNOWFLAKE_PRIVATE_KEY_PASSPHASE}" \
  snowsql \
  --accountname "$(echo "${SNOWFLAKE_SERVER_NAME}" | cut -d "." -f 1-2)" \
  --username "${SNOWFLAKE_USERNAME}" \
  --private-key-path "rsa_key.p8" \
  --query "
  SELECT SYSTEM\$PIPE_FORCE_RESUME('${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${SNOWFLAKE_PIPE}');
"
# [END]

# [START dynamic_variable_pubsub]
GCP_PROJECT_ID="$(gcloud config get-value core/project)"
PIPELINE_PUBSUB_TOPIC_FQN="projects/${GCP_PROJECT_ID}/topics/${PIPELINE_PUBSUB_TOPIC}"
PIPELINE_PUBSUB_SUBSCRIPTION_FQN="projects/${GCP_PROJECT_ID}/subscriptions/${PIPELINE_PUBSUB_SUBSCRIPTION}"
# [END]

if ! gcloud pubsub topics describe "${PIPELINE_PUBSUB_TOPIC_FQN}" ; then
# [START create_pubsub]
gcloud pubsub topics create "${PIPELINE_PUBSUB_TOPIC_FQN}"
gcloud pubsub subscriptions create --topic "${PIPELINE_PUBSUB_TOPIC_FQN}" "${PIPELINE_PUBSUB_SUBSCRIPTION_FQN}"
# [END]
fi

# [START create_schema]
echo '{
   "id": "{{uuid()}}",
   "name": "A green door",
   "age": {{integer(1,50)}},
   "price": 12.50
}' | gsutil cp - "gs://${DATAFLOW_BUCKET}/stream-schema.json"
# [END]
