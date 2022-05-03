#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

set -x

./mvnw package -P "dataflow-runner" --batch-mode
./javaw -jar target/ingest-pubsub-to-snowflake-bundled-1.0.jar --help

./mvnw package -P "direct-runner" --batch-mode
./javaw -jar target/ingest-pubsub-to-snowflake-bundled-1.0.jar --help
