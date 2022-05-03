#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

if ! command -v docker > /dev/null; then
  echo "docker not installed."
  exit 1
fi

mkdir -p "$HOME/.m2"

exec docker run \
    --user "$(id -u):$(id -g)" \
    -v "$HOME/.m2:/cache/.m2" \
    -v "$PWD:/app" \
    -w /app \
    -e "MAVEN_OPTS=-Dmaven.repo.local=/cache/.m2" \
    maven:3.8.3-jdk-11 mvn ${@};
