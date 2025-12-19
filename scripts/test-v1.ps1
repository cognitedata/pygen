# Run Pygen v1 (legacy) tests

Write-Host "Running Pygen v1 (Legacy) Tests..." -ForegroundColor Cyan
uv run pytest legacy/tests/ $args

