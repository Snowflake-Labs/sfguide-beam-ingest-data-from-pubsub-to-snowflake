#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

export $(cat .env | grep -v "^#" | xargs)

set -x
./run_generator.sh &
GENERATOR_PID=$!

./mvnw clean package -P "direct-runner" --batch-mode

./run_pipeline_on_direct_runner.sh &
PIPELINE_PID=$!
sleep 5

while : ; do
  element_count=$(snowsql \
    -c "${SNOWSQL_CONN}" \
    -q "SELECT COUNT(*) FROM ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${PIPELINE_SNOWFLAKE_OUTPUT_TABLE}" \
    -o output_format=tsv \
    -o header=false \
    -o timing=false \
    -o friendly=false)
  echo "Element count: ${element_count}/100"
  if [ "${element_count}" -gt "100" ]; then
      break
  fi
  sleep 5;
done
kill "${PIPELINE_PID}"
kill "${GENERATOR_PID}"
sleep 3
echo "Finished"