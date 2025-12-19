#!/bin/bash
# Run Pygen v2 tests

echo "Running Pygen v2 Tests..."
uv run pytest tests/ "$@"

