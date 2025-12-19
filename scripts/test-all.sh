#!/bin/bash
# Run both v1 and v2 tests

echo "Running All Tests (v1 + v2)..."
echo ""
echo "=== V2 Tests ==="
pytest tests/ "$@"
v2_exit=$?

echo ""
echo "=== V1 (Legacy) Tests ==="
pytest legacy/tests/ "$@"
v1_exit=$?

echo ""
if [ $v2_exit -eq 0 ] && [ $v1_exit -eq 0 ]; then
    echo "✅ All tests passed!"
    exit 0
else
    echo "❌ Some tests failed."
    exit 1
fi

