# Run both v1 and v2 tests

Write-Host "Running All Tests (v1 + v2)..." -ForegroundColor Cyan
Write-Host ""

Write-Host "=== V2 Tests ===" -ForegroundColor Yellow
uv run pytest tests/ $args
$v2Exit = $LASTEXITCODE

Write-Host ""
Write-Host "=== V1 (Legacy) Tests ===" -ForegroundColor Yellow
uv run pytest legacy/tests/ $args
$v1Exit = $LASTEXITCODE

Write-Host ""
if ($v2Exit -eq 0 -and $v1Exit -eq 0) {
    Write-Host "✅ All tests passed!" -ForegroundColor Green
    exit 0
} else {
    Write-Host "❌ Some tests failed." -ForegroundColor Red
    exit 1
}

