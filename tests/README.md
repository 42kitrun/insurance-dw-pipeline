# Insurance DW Pipeline Integration Tests

Comprehensive integration tests for all 5 DAGs in the insurance data warehouse ETL pipeline.

## Test Coverage

### 1. **Dimension DAG** - `test_init_dim_date.py`
Tests for the `init_dim_date` DAG that initializes the date dimension table.

**Tests:**
- Table structure validation (correct columns and data types)
- Correct number of records loaded (4,018 dates from 2020-2030)
- Date range validation (2020-01-01 to 2030-12-31)
- Date key format validation (YYYYMMDD)
- Weekend flag calculation accuracy
- Quarter calculation correctness
- Duplicate prevention (primary key constraint)

### 2. **Data Quality DAG** - `test_data_quality_checks.py`
Tests for the `insurance_data_quality_checks` DAG that validates data integrity.

**Tests:**
- DAG structure and scheduling (runs daily at 02:45)
- Quality rules coverage for all 11 fact tables
- NULL value detection in required columns
- Duplicate record detection
- Validation of specific table rules (life_general, auto_loss, nonlife_finance)
- Quality check passes with valid data

### 3. **Data Mart DAG** - `test_monthly_summary_mart.py`
Tests for the `insurance_monthly_summary_mart` DAG that aggregates data.

**Tests:**
- DAG structure and scheduling (runs daily at 03:00)
- Table structure validation
- Successful mart building with sample data
- Correct aggregation logic (sum of employees, max assets, sum of contracts)
- TRUNCATE behavior before reload
- Handling of missing data via LEFT JOINs
- Default value assignment (COALESCE)

### 4. **ETL DAGs** - `test_etl_dags.py`
Tests for the three API-backed ETL DAGs (auto, life, nonlife insurance).

**Auto Insurance ETL Tests:**
- DAG structure and scheduling (02:00 daily)
- Presence of all 3 load tasks (contract, loss, victim)
- Table structures for fact_auto_contract, fact_auto_loss, fact_auto_victim
- Mock API response handling

**Life Insurance ETL Tests:**
- DAG structure and scheduling (02:15 daily)
- Presence of all 4 load tasks (general, finance, business, KPI)
- Table structures for all life insurance fact tables

**Non-life Insurance ETL Tests:**
- DAG structure and scheduling (02:30 daily)
- Presence of all 4 load tasks (general, finance, business, KPI)
- Table structures for all non-life insurance fact tables

**Shared ETL Tests:**
- Scheduling stagger (prevents resource contention)
- Catchup disabled for all DAGs
- Start dates properly configured

## Running Tests

### Prerequisites
```bash
# Install test dependencies
uv pip install pytest pytest-cov pytest-mock

# Set up test database connection
export TEST_DB_HOST=localhost
export TEST_DB_PORT=5432
export TEST_DB_NAME=insurance_dw
export TEST_DB_USER=postgres
export TEST_DB_PASSWORD=<password>
```

### Run All Tests
```bash
pytest tests/
```

### Run Specific Test File
```bash
pytest tests/test_init_dim_date.py -v
pytest tests/test_data_quality_checks.py -v
pytest tests/test_monthly_summary_mart.py -v
pytest tests/test_etl_dags.py -v
```

### Run Specific Test
```bash
pytest tests/test_init_dim_date.py::test_dim_date_load_records -v
```

### Run with Coverage Report
```bash
pytest tests/ --cov=dags --cov-report=html
```

### Run Only Integration Tests
```bash
pytest tests/ -m integration -v
```

## Test Architecture

### Fixtures (`conftest.py`)
- **postgres_connection**: Establishes test DB connection
- **pg_hook**: Provides Airflow PostgreSQL hook
- **db_cursor**: Database cursor with cleanup
- **clean_tables**: Truncates fact tables before/after tests
- **sample_date_key**: Provides test date in YYYYMMDD format

### Test Data Flow
1. **Setup**: `clean_tables` fixture truncates all fact tables
2. **Insert**: Tests insert sample data into specific tables
3. **Execute**: DAG task is executed
4. **Verify**: Assertions check results
5. **Cleanup**: `clean_tables` fixture truncates tables again

## Key Test Scenarios

### Data Quality Validation
- NULL values in required columns
- Duplicate detection across unique key combinations
- Data type mismatches

### Aggregation Correctness
- SUM operations (employee counts, contract counts)
- MAX operations (asset amounts)
- COALESCE default values

### Schema Compliance
- Column existence and naming
- Data type validation
- Constraint enforcement (primary keys, foreign keys)

### Schedule Coordination
- ETL DAGs run in staggered 15-minute intervals
- Quality checks run after ETL completion
- Mart rebuild runs after quality validation

## Debugging Tests

### Print Debug Info
```python
def test_something(pg_hook, db_cursor):
    cursor = pg_hook.get_conn().cursor()
    cursor.execute("SELECT * FROM fact_table LIMIT 5")
    print(cursor.fetchall())  # Debug output
```

### Run Single Test with Print Output
```bash
pytest tests/test_init_dim_date.py::test_dim_date_load_records -v -s
```

### Check Database State
```bash
psql -h localhost -U postgres -d insurance_dw -c "SELECT COUNT(*) FROM dim_date;"
```

## Continuous Integration

Add to CI/CD pipeline:
```bash
# Run tests before deployment
pytest tests/ --tb=short --junitxml=test-results.xml

# Fail if coverage drops below 80%
pytest tests/ --cov=dags --cov-fail-under=80
```

## Maintenance

- **Add new DAG tests**: Create `test_<dag_name>.py` file following existing patterns
- **Update fixtures**: Modify `conftest.py` if new capabilities needed
- **Expand test data**: Add more sample data scenarios in test methods
- **Monitor flaky tests**: Use `--lf` to rerun last failed tests
