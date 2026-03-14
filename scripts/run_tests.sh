#!/bin/bash
# Test runner script for Insurance DW Pipeline integration tests

set -e

echo "================================================"
echo "Insurance DW Pipeline - Integration Test Suite"
echo "================================================"
echo ""

# Check pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "❌ pytest not found. Installing test dependencies..."
    uv pip install pytest pytest-cov pytest-mock
fi

# Show test configuration
echo "📋 Test Configuration:"
echo "   Test DB Host: ${TEST_DB_HOST:-localhost}"
echo "   Test DB Port: ${TEST_DB_PORT:-5432}"
echo "   Test DB Name: ${TEST_DB_NAME:-insurance_dw}"
echo "   Test DB User: ${TEST_DB_USER:-postgres}"
echo ""

# Parse command line arguments
COVERAGE=false
SPECIFIC_TEST=""
MARKERS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --coverage)
            COVERAGE=true
            shift
            ;;
        --etl)
            MARKERS="-m etl"
            shift
            ;;
        --quality)
            MARKERS="-m quality"
            shift
            ;;
        --mart)
            MARKERS="-m mart"
            shift
            ;;
        --dim)
            MARKERS="-m dim"
            shift
            ;;
        --test)
            SPECIFIC_TEST="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--coverage] [--etl|--quality|--mart|--dim] [--test test_name]"
            exit 1
            ;;
    esac
done

# Build pytest command
PYTEST_CMD="pytest tests/ -v"

if [ -n "$MARKERS" ]; then
    PYTEST_CMD="$PYTEST_CMD $MARKERS"
fi

if [ -n "$SPECIFIC_TEST" ]; then
    PYTEST_CMD="pytest tests/$SPECIFIC_TEST -v"
fi

if [ "$COVERAGE" = true ]; then
    PYTEST_CMD="$PYTEST_CMD --cov=dags --cov-report=html --cov-report=term-missing"
    echo "📊 Coverage report will be generated"
fi

echo ""
echo "🚀 Running tests..."
echo "   Command: $PYTEST_CMD"
echo ""

# Run tests
eval $PYTEST_CMD
TEST_EXIT=$?

echo ""
if [ $TEST_EXIT -eq 0 ]; then
    echo "✅ All tests passed!"
    if [ "$COVERAGE" = true ]; then
        echo "📊 Coverage report: htmlcov/index.html"
    fi
else
    echo "❌ Some tests failed. Exit code: $TEST_EXIT"
fi

exit $TEST_EXIT
