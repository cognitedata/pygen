# Run Pygen v2 tests

Write-Host "Running Pygen v2 Tests..." -ForegroundColor Cyan
uv run pytest tests/ $args

