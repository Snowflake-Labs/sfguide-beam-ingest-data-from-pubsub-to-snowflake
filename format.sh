#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

GOOGLE_JAVA_FORMAT_VERSION="1.15.0"
DOWNLOAD_URL="https://github.com/google/google-java-format/releases/download/v${GOOGLE_JAVA_FORMAT_VERSION}/google-java-format-${GOOGLE_JAVA_FORMAT_VERSION}-all-deps.jar"
JAR_FILE="./.cache/google-java-format-${GOOGLE_JAVA_FORMAT_VERSION}-all-deps.jar"

if [ ! -f "${JAR_FILE}" ]; then
  mkdir -p "$(dirname "${JAR_FILE}")"
  echo "Downloading Google Java format to ${JAR_FILE}"
  curl -# -L --fail "${DOWNLOAD_URL}" --output "${JAR_FILE}"
fi

if ! command -v java > /dev/null; then
  echo "Java not installed."
  exit 1
fi
echo "Running Google Java Format"
find ./src -type f -name "*.java" -print0 | xargs -0 java -jar "${JAR_FILE}" --replace --set-exit-if-changed && echo "OK"
