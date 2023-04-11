#!/bin/bash
set -e

. "${0%/*}/_functions.sh" "$1"


SCHEMA_FILE=$(cat "$CONFIG" | extract 'schema_file')
SPACE=$(cat "$CONFIG" | extract 'space')
DATAMODEL=$(cat "$CONFIG" | extract 'datamodel')
SCHEMA_VERSION=$(cat "$CONFIG" | extract 'schema_version')

exec cdf data-models publish --file="$SCHEMA_FILE" --space="$SPACE" --external-id="$DATAMODEL" --version="$SCHEMA_VERSION"
