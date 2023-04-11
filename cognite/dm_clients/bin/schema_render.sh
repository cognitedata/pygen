#!/bin/bash
set -e

. "${0%/*}/_functions.sh" "$1"


SCHEMA_FILE=$(cat "$CONFIG" | extract 'schema_file')
SCHEMA_MODULE=$(cat "$CONFIG" | extract 'schema_module')

cat <<'EOF' > "$SCHEMA_FILE"
# THIS FILE IS AUTO-GENERATED!
# Use `dm_clients schema render` to update it, see `dm_clients --help` for more information.


EOF

python -m "$SCHEMA_MODULE" >> "$SCHEMA_FILE"
