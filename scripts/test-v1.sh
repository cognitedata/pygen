#!/bin/bash
# Run Pygen v1 (legacy) tests

echo "Running Pygen v1 (Legacy) Tests..."
uv run pytest legacy/tests/ "$@"

