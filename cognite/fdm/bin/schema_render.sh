#!/bin/bash
set -e

. fdm/bin/_functions.sh "$1"

SCHEMA_FILE=$(cat "$CONFIG" | extract 'schema_file')
SCHEMA_MODULE=$(cat "$CONFIG" | extract 'schema_module')

cat <<'EOF' > "$SCHEMA_FILE"
# THIS FILE IS AUTO-GENERATED!
# Use fdm/bin/schema_render.sh to update it.


EOF

# skip initial lines which define schema{query:MyRootModel}, CDF doesn't understand it:
SKIP_LINES=5

python -m "$SCHEMA_MODULE" | tail -n +$SKIP_LINES >> "$SCHEMA_FILE"
