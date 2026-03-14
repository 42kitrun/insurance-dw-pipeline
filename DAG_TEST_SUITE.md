# Insurance DW Pipeline - 5 DAG Integration Test Suite

## Overview

A comprehensive integration test suite for all 5 DAGs in the insurance data warehouse pipeline:

1. **init_dim_date** - Date dimension initialization
2. **insurance_data_quality_checks** - Data quality validation
3. **insurance_monthly_summary_mart** - Mart aggregation
4. **auto_insurance_etl** - Auto insurance API ETL
5. **life_insurance_etl** - Life insurance API ETL
6. **nonlife_insurance_etl** - Non-life insurance API ETL (bonus)

## Test Files Created

```
tests/
├── __init__.py                          # Test package initialization
├── conftest.py                          # Shared pytest fixtures
├── test_init_dim_date.py               # 9 tests for dim_date DAG
├── test_data_quality_checks.py         # 8 tests for quality checks DAG
├── test_monthly_summary_mart.py        # 6 tests for mart DAG
├── test_etl_dags.py                    # 18 tests for all ETL DAGs
├── requirements.txt                     # Test dependencies
└── README.md                            # Detailed test documentation

scripts/
└── run_tests.sh                         # Convenient test runner script

pytest.ini                               # Pytest configuration
DAG_TEST_SUITE.md                       # This file
```

## Quick Start

### 1. Install Test Dependencies

```bash
# Using uv (recommended)
uv pip install -r tests/requirements.txt

# Or using pip directly
pip install pytest pytest-cov pytest-mock psycopg2-binary
```

### 2. Set Up Test Database

The tests use your existing `insurance_rds` connection. Ensure PostgreSQL is running and accessible.

```bash
# Optional: Set custom database connection
export TEST_DB_HOST=localhost
export TEST_DB_PORT=5432
export TEST_DB_NAME=insurance_dw
export TEST_DB_USER=postgres
export TEST_DB_PASSWORD=<your-password>
```

### 3. Run All Tests

```bash
# Run all 41 tests
pytest tests/

# Or use the convenient script
./scripts/run_tests.sh

# With coverage report
./scripts/run_tests.sh --coverage
```

## Test Coverage Summary

| DAG | Test File | # Tests | Focus Areas |
|-----|-----------|---------|-------------|
| **init_dim_date** | test_init_dim_date.py | 9 | Schema, date ranges, formats, constraints |
| **insurance_data_quality_checks** | test_data_quality_checks.py | 8 | NULL checks, duplicates, rule validation |
| **insurance_monthly_summary_mart** | test_monthly_summary_mart.py | 6 | Aggregations, TRUNCATE, LEFT JOINs |
| **auto_insurance_etl** | test_etl_dags.py | 6 | DAG structure, task existence, tables |
| **life_insurance_etl** | test_etl_dags.py | 6 | DAG structure, task existence, tables |
| **nonlife_insurance_etl** | test_etl_dags.py | 6 | DAG structure, task existence, tables |
| **Shared ETL Tests** | test_etl_dags.py | 3 | Scheduling, catchup, start dates |
| **Total** | | **41** | Complete pipeline validation |

## Test Categories

### Dimension Table Tests (9 tests)
- ✅ Schema validation
- ✅ Record count (4,018 dates)
- ✅ Date range (2020-01-01 to 2030-12-31)
- ✅ Date key format (YYYYMMDD)
- ✅ Weekend flag accuracy
- ✅ Quarter calculation
- ✅ No duplicates
- ✅ Primary key constraint
- ✅ All date components populated

### Data Quality Tests (8 tests)
- ✅ Quality rules defined for all tables
- ✅ NULL value detection
- ✅ Duplicate detection by unique keys
- ✅ Table-specific rule validation
- ✅ Integration with fact tables
- ✅ Pass/fail scenarios

### Data Mart Tests (6 tests)
- ✅ Correct aggregation logic
- ✅ TRUNCATE before reload behavior
- ✅ LEFT JOIN handling with defaults
- ✅ COALESCE for missing data
- ✅ Multi-table joins
- ✅ Empty state handling

### ETL DAG Tests (18 tests)
- ✅ DAG structure validation
- ✅ Task existence checks
- ✅ Table schema validation
- ✅ API endpoint configuration
- ✅ Data mapping
- ✅ Scheduling validation
- ✅ Staggered execution (02:00, 02:15, 02:30)
- ✅ Catchup disabled
- ✅ Start date configuration

## Test Architecture

### Shared Fixtures (conftest.py)
```python
@pytest.fixture
def pg_hook()              # Airflow PostgreSQL hook
def db_cursor()            # Database cursor with cleanup
def clean_tables()         # Truncate fact tables
def sample_date_key()      # Test date (YYYYMMDD format)
```

### Test Data Flow
1. **Setup**: Fixtures truncate tables
2. **Insert**: Tests insert sample data
3. **Execute**: DAG or task runs
4. **Verify**: Assertions check results
5. **Cleanup**: Fixtures clean up after test

## Running Specific Tests

```bash
# Test init_dim_date DAG only
pytest tests/test_init_dim_date.py -v

# Test quality checks only
pytest tests/test_data_quality_checks.py -v

# Test mart DAG only
pytest tests/test_monthly_summary_mart.py -v

# Test auto insurance ETL
pytest tests/test_etl_dags.py::TestAutoInsuranceETL -v

# Test single test case
pytest tests/test_init_dim_date.py::test_dim_date_load_records -v

# With coverage
pytest tests/ --cov=dags --cov-report=html

# With test output visible
pytest tests/ -v -s
```

## Using the Test Runner Script

```bash
# Run all tests
./scripts/run_tests.sh

# Run with coverage report
./scripts/run_tests.sh --coverage

# Run only specific DAG type
./scripts/run_tests.sh --dim      # dimension tests
./scripts/run_tests.sh --quality  # quality check tests
./scripts/run_tests.sh --mart     # mart tests
./scripts/run_tests.sh --etl      # ETL tests only

# Run specific test file
./scripts/run_tests.sh --test test_init_dim_date.py
```

## Database State During Tests

The test suite uses the following approach:
1. **Before each test**: Tables are TRUNCATED via `clean_tables` fixture
2. **During test**: Sample data is inserted and DAGs are executed
3. **After each test**: Tables are TRUNCATED again for isolation
4. **Between tests**: No data persists (fresh state guaranteed)

This ensures tests are **isolated and repeatable**.

## Debugging Failed Tests

### View Test Output
```bash
pytest tests/test_init_dim_date.py::test_dim_date_load_records -vv -s
```

### Check Database Directly
```bash
psql -h localhost -U postgres -d insurance_dw
```

```sql
-- Check dim_date
SELECT COUNT(*) FROM dim_date;
SELECT * FROM dim_date LIMIT 5;

-- Check fact tables
SELECT COUNT(*) FROM fact_auto_contract;
SELECT * FROM fact_life_general LIMIT 5;

-- Check mart
SELECT * FROM mart_monthly_summary;
```

### Inspect Test Code
```bash
# View specific test
cat tests/test_init_dim_date.py | grep -A 20 "def test_dim_date_load_records"
```

## Integration with CI/CD

Add to your GitHub Actions / GitLab CI / Jenkins pipeline:

```yaml
# Example: GitHub Actions
- name: Run DAG Integration Tests
  run: |
    pip install -r tests/requirements.txt
    pytest tests/ --tb=short --junitxml=test-results.xml

- name: Upload Coverage
  uses: codecov/codecov-action@v3
  with:
    files: ./coverage.xml
```

## Expected Test Results

When all tests pass, you should see:
```
test_init_dim_date.py::test_dim_date_table_structure PASSED
test_init_dim_date.py::test_dim_date_load_records PASSED
... (39 more tests)
================================ 41 passed in 2.34s ================================
```

## Key Testing Insights

### What These Tests Validate

✅ **Data Integrity**
- Tables have correct schema
- Data types match expectations
- Constraints are enforced

✅ **DAG Functionality**
- All tasks exist and are defined
- Schedules are staggered to prevent conflicts
- Data loads end-to-end

✅ **ETL Correctness**
- Dimension tables have complete data
- Quality checks catch data issues
- Aggregations produce correct results

✅ **Pipeline Reliability**
- No duplicate records loaded
- Missing data handled gracefully
- TRUNCATE/RELOAD works correctly

### What These Tests DON'T Cover

❌ External API responses (mocked)
❌ Network failures (not in integration scope)
❌ Airflow scheduler behavior (use Airflow's own testing)
❌ Production performance (use profiling tools)

## Extending the Test Suite

### Add Test for New DAG

1. Create `tests/test_my_new_dag.py`
2. Use existing test patterns as template
3. Define fixtures needed
4. Write assertions to validate behavior
5. Run: `pytest tests/test_my_new_dag.py -v`

### Add Test for New Table

Update `clean_tables` fixture in `conftest.py`:
```python
tables = [
    ...,
    "my_new_table",  # Add here
]
```

Then add table-specific tests to appropriate test file.

## Performance Notes

- **Test suite runtime**: ~2-5 seconds (depending on DB connection)
- **Database operations**: ~100-500ms per test
- **Total tests**: 41
- **Parallel execution**: Can be enabled with pytest-xdist if needed

## Troubleshooting

### "FATAL: password authentication failed"
```bash
# Check database connection settings
echo $TEST_DB_HOST $TEST_DB_PORT $TEST_DB_NAME $TEST_DB_USER
# Update password if needed
export TEST_DB_PASSWORD=<correct-password>
```

### "table does not exist"
```bash
# Ensure schema is created
python scripts/apply_ddl_to_rds.py
# Or recreate tables manually
psql -h $TEST_DB_HOST -U $TEST_DB_USER -d $TEST_DB_NAME -f sql/ddl/create_schema.sql
```

### "ModuleNotFoundError: No module named 'dags'"
```bash
# Ensure dags directory is in PYTHONPATH
export PYTHONPATH=/Users/seSAC/src/insurance-dw-pipeline:$PYTHONPATH
pytest tests/
```

### Tests pass locally but fail in CI
- Check CI environment variables match local setup
- Verify database is accessible from CI runner
- Check timezone settings (tests use pendulum)
- Verify Python/package versions match

## Next Steps

1. ✅ Run: `pytest tests/` to validate your setup
2. ✅ Review: `tests/README.md` for detailed documentation
3. ✅ Integrate: Add tests to your CI/CD pipeline
4. ✅ Extend: Add tests as you add new DAGs/features
5. ✅ Monitor: Track test coverage over time

## Support

For issues or questions:
1. Check `tests/README.md` for detailed test documentation
2. Review individual test files for implementation details
3. Run tests with `-vv -s` flags for debug output
4. Check database state directly with psql
