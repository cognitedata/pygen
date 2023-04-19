#!/bin/bash
set -e

. "${0%/*}/_functions.sh" "$1"


SCHEMA_FILE=$(cat "$CONFIG" | extract 'schema_file')
SCHEMA_MODULE=$(cat "$CONFIG" | extract 'schema_module')

python -m "$SCHEMA_MODULE" >> "$SCHEMA_FILE"
