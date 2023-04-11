#!/bin/bash
set -e

. "${0%/*}/_functions.sh" "$1"


SCHEMA_FILE=$(cat "$CONFIG" | extract 'schema_file')
SCHEMA_MODULE=$(cat "$CONFIG" | extract 'schema_module')

cat <<'EOF' > "$SCHEMA_FILE"
# THIS FILE IS AUTO-GENERATED!
# Use fdm/bin/schema_render.sh to update it.


EOF

python -m "$SCHEMA_MODULE" >> "$SCHEMA_FILE"
