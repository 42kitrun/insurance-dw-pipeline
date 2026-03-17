# Insurance Data Warehouse Pipeline

[![Python 3.13+](https://img.shields.io/badge/python-3.13+-3776ab.svg?style=flat-square&logo=python)](https://www.python.org/)
[![Apache Airflow 3.1.7](https://img.shields.io/badge/airflow-3.1.7-017cee.svg?style=flat-square&logo=apache-airflow)](https://airflow.apache.org/)
[![PostgreSQL 15](https://img.shields.io/badge/postgresql-15-336791.svg?style=flat-square&logo=postgresql)](https://www.postgresql.org/)
[![Docker Compose](https://img.shields.io/badge/docker-compose-2496ed.svg?style=flat-square&logo=docker)](https://docs.docker.com/compose/)
[![Tests Passing](https://img.shields.io/badge/tests-45%20passing-success.svg?style=flat-square)](./tests/)
[![License MIT](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)](LICENSE)

A comprehensive **Apache Airflow-based ETL pipeline** for insurance industry data processing. Ingests data from multiple public APIs, transforms it into a star schema data warehouse, performs quality checks, and generates analytical data marts.

## 🎯 Features

- **7 Apache Airflow DAGs** for orchestrated data pipelines
- **Star Schema Data Warehouse** with 14 tables (4 dimensions, 11 facts)
- **Real-time Data Quality Checks** for NULL values and duplicate detection
- **Multi-source ETL** from auto/life/nonlife insurance APIs
- **45+ Integration Tests** with 100% pass rate
- **Docker-based Local Development** environment with PostgreSQL
- **AWS RDS PostgreSQL** production deployment ready

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Data Sources (Public APIs)                │
├──────────────────────────┬──────────────────────┬───────────┤
│  Auto Insurance API      │  Life Insurance API  │  Non-life │
│  (Vehicle Claims)        │  (General Overview)  │  API      │
└──────────────┬───────────┴──────────┬───────────┴─────┬─────┘
               │                      │                 │
               v                      v                 v
┌─────────────────────────────────────────────────────────────┐
│              Apache Airflow Orchestration Layer             │
├─────────────────┬──────────────────┬──────────┬────────────┤
│  ETL DAGs       │  Quality Checks  │  Mart    │  Init      │
│  (3x)           │  DAG             │  DAG     │  DAG       │
└─────────────────┴──────────────────┴──────────┴────┬───────┘
                                                     │
               ┌─────────────────────────────────────v────────────┐
               │      PostgreSQL Data Warehouse (insurance_dw)    │
               ├──────────────────────┬──────────────────────────┤
               │  STAR SCHEMA         │  FACT TABLES (11)        │
               │  ├─ dim_date         │  ├─ fact_auto_contract  │
               │  ├─ dim_insurance_   │  ├─ fact_auto_loss      │
               │  │  company          │  ├─ fact_auto_victim    │
               │  ├─ dim_insurance_   │  ├─ fact_life_general   │
               │  │  type             │  ├─ fact_life_finance   │
               │  └─ [Degenerate]     │  ├─ fact_nonlife_*      │
               │                      │  └─ [& more...]         │
               └──────────────┬───────┴──────────────────────────┘
                              │
               ┌──────────────v────────────────┐
               │  Analytics Data Mart          │
               │  mart_monthly_summary         │
               │  (Aggregated Metrics)         │
               └───────────────────────────────┘
```

### Data Flow

1. **Ingestion**: Public APIs → Airflow DAGs
2. **Transformation**: Raw data → Star schema dimensions & facts
3. **Quality**: Automated validation with duplicate/NULL detection
4. **Analytics**: Aggregated monthly summary mart
5. **Output**: Ready for BI tools (Tableau, Power BI, etc.)

---

## 🚀 Quick Start

### Prerequisites

- **Docker & Docker Compose** (for local development)
- **Python 3.13+** (for local Airflow setup)
- **PostgreSQL 15** (included in docker-compose)

### Option 1: Docker Compose (Recommended)

```bash
# Clone and navigate
cd insurance-dw-pipeline

# Start services (Airflow + PostgreSQL)
docker-compose up -d

# Wait for Airflow to initialize (60 seconds)
sleep 60

# Access Airflow UI
open http://localhost:8080

# Login credentials
# Username: admin
# Password: admin
```

### Option 2: Local Development

```bash
# Install dependencies
uv pip install -r requirements.txt

# Set up PostgreSQL connection
export AIRFLOW_CONN_INSURANCE_RDS="postgresql://postgres:password@localhost:5432/insurance_dw"

# Initialize Airflow
airflow db migrate

# Start scheduler & webserver
airflow scheduler &
airflow webserver
```

---

## 📋 DAGs Overview

### 1. **init_dim_date** (Initialization)
- **Frequency**: Manual trigger
- **Purpose**: Load date dimension (2020-2030)
- **Records**: 4,018 daily records
- **Output**: dim_date table with date attributes (quarter, week, weekend flag, etc.)

```python
# Table: dim_date
date_key (YYYYMMDD) | full_date | year | month | quarter | is_weekend
20200101            | 2020-01-01| 2020 | 1     | 1       | false
20200102            | 2020-01-02| 2020 | 1     | 1       | false
```

### 2. **auto_insurance_etl** (Daily ETL)
- **Frequency**: Daily at 00:00 UTC
- **Source**: Public Auto Insurance API (data.go.kr)
- **Tables**: 3 fact tables
  - `fact_auto_contract` - Vehicle insurance contracts
  - `fact_auto_loss` - Claims and loss information
  - `fact_auto_victim` - Accident victim details

### 3. **life_insurance_etl** (Daily ETL)
- **Frequency**: Daily at 01:00 UTC
- **Source**: Public Life Insurance API
- **Tables**: 2 fact tables
  - `fact_life_general` - General company overview
  - `fact_life_finance` - Financial metrics

### 4. **nonlife_insurance_etl** (Daily ETL)
- **Frequency**: Daily at 02:00 UTC
- **Source**: Public Non-life Insurance API
- **Tables**: 6 fact tables
  - `fact_nonlife_*` - Various non-life insurance metrics

### 5. **insurance_data_quality_checks** (Daily Validation)
- **Frequency**: Daily at 03:00 UTC (after all ETL)
- **Validation Rules**:
  - NULL value detection in required columns
  - Duplicate detection across unique key combinations
  - Data completeness checks
- **Scope**: All 11 fact tables

### 6. **insurance_monthly_summary_mart** (Monthly Aggregation)
- **Frequency**: Monthly at 04:00 on 1st day
- **Purpose**: Generate analytical mart
- **Output**: `mart_monthly_summary` table with:
  - Monthly contract counts by insurance type
  - Aggregated premium and loss amounts
  - Employee metrics by company

### 7. **insurance_quality_rules** (Configuration)
- **Runtime**: Embedded in DAGs
- **Purpose**: Define business rules for validation

---

## 🗂️ Project Structure

```
insurance-dw-pipeline/
├── dags/
│   ├── init_dim_date.py                 # Date dimension initialization
│   ├── auto_insurance_etl.py           # Auto insurance ETL
│   ├── life_insurance_etl.py           # Life insurance ETL
│   ├── nonlife_insurance_etl.py        # Non-life insurance ETL
│   ├── data_quality_checks.py          # Quality validation DAG
│   ├── monthly_summary_mart.py         # Monthly mart aggregation
│   └── insurance_dw_common.py          # Shared utilities & schemas
│
├── sql/
│   └── ddl/
│       └── create_schema.sql           # Star schema DDL (14 tables)
│
├── scripts/
│   ├── apply_ddl_to_rds.py            # Deploy schema to AWS RDS
│   ├── verify_dim_date.py             # Data validation script
│   └── run_tests.sh                   # Test runner utility
│
├── tests/
│   ├── conftest.py                    # Pytest fixtures & DB setup
│   ├── test_init_dim_date.py         # 9 dimension tests
│   ├── test_data_quality_checks.py   # 8 quality check tests
│   ├── test_monthly_summary_mart.py  # 6 mart aggregation tests
│   ├── test_etl_dags.py              # 18 ETL DAG tests
│   ├── requirements.txt               # Test dependencies
│   └── README.md                      # Detailed test documentation
│
├── config/
│   └── simple_auth_manager_passwords.json  # Airflow auth
│
├── docker-compose.yml                 # Local dev environment
├── pyproject.toml                     # Project metadata & dependencies
├── pytest.ini                         # Pytest configuration
├── DAG_TEST_SUITE.md                 # Test suite documentation
└── README.md                         # This file
```

---

## 💾 Database Schema

### Star Schema (14 Tables)

```
DIMENSION TABLES (4):
  dim_date                  - Date attributes (2020-2030)
  dim_insurance_company     - Insurance companies
  dim_insurance_type        - Insurance product types
  dim_[Degenerate]          - Industry classification

FACT TABLES (11):
  ✅ Auto Insurance (3 tables)
     fact_auto_contract    - Vehicle insurance policies
     fact_auto_loss        - Claims and losses
     fact_auto_victim      - Accident victims

  ✅ Life Insurance (2 tables)
     fact_life_general     - Company general info
     fact_life_finance     - Financial metrics

  ✅ Non-life Insurance (6 tables)
     fact_nonlife_*        - Various non-life metrics

MART TABLES (1):
  mart_monthly_summary      - Monthly aggregated analytics

Total: 15 tables (Dimensions + Facts + Marts)
```

### Key Columns (Common to All Facts)

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL PRIMARY KEY | Row identifier |
| `date_key` | CHAR(8) FK → dim_date | Date reference (YYYYMMDD) |
| `created_at` | TIMESTAMP | Record creation timestamp |

---

## 🧪 Testing

### 45 Integration Tests (100% Pass Rate)

```bash
# Run all tests
pytest tests/

# Run with coverage report
./scripts/run_tests.sh --coverage

# Run specific test category
./scripts/run_tests.sh --etl        # ETL DAG tests (18)
./scripts/run_tests.sh --quality    # Quality check tests (8)
./scripts/run_tests.sh --mart       # Mart tests (6)
./scripts/run_tests.sh --dim        # Dimension tests (9)

# Run specific test file
./scripts/run_tests.sh --test tests/test_init_dim_date.py
```

### Test Coverage Summary

| Category | Tests | Focus Areas |
|----------|-------|-------------|
| **Dimensions** | 9 | Schema, date ranges, formats, uniqueness |
| **Quality** | 8 | NULL detection, duplicates, validation rules |
| **Marts** | 6 | Aggregations, JOINs, data consistency |
| **ETL DAGs** | 18 | Structure, tasks, scheduling, schemas |
| **Shared** | 4 | Configuration, dependencies, utilities |
| **Total** | **45** | Full pipeline validation |

### Test Infrastructure

- **Framework**: pytest with custom fixtures
- **Database**: PostgreSQL (insurance_dw test schema)
- **Isolation**: Automatic table cleanup between tests
- **Performance**: ~89 seconds for full suite

---

## 🔧 Configuration

### Environment Variables

```bash
# Database Connection (for local PostgreSQL)
AIRFLOW_CONN_INSURANCE_RDS=postgresql://postgres:password@localhost:5432/insurance_dw

# AWS RDS Connection (for production)
AIRFLOW_CONN_INSURANCE_RDS=postgresql://admin:password@insurance-dw.c12345abcdef.us-east-1.rds.amazonaws.com:5432/insurance_dw

# API Key (required for real data ingestion)
DATA_GO_KR_API_KEY=your_api_key_here

# Airflow Configuration
AIRFLOW_HOME=/opt/airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
```

### Database Credentials

| Component | User | Password | Database |
|-----------|------|----------|----------|
| Airflow | airflow | airflow | airflow |
| Data WH | postgres | your_password | insurance_dw |
| Test DB | postgres | your_password | insurance_dw |

---

## 📈 Performance Metrics

| Metric | Value | Notes |
|--------|-------|-------|
| **DAGs** | 7 | Orchestrated pipelines |
| **Tables** | 15 | Dimensions + Facts + Marts |
| **Daily Records** | ~50K-200K | Varies by data source |
| **Test Suite** | 45 tests | 89 seconds execution |
| **Test DB Size** | ~500MB | Full schema + test data |
| **Data Retention** | 11 years | 2020-2030 (configurable) |

---

## 🚢 Deployment

### AWS RDS PostgreSQL

```bash
# 1. Create RDS instance
aws rds create-db-instance \
  --db-instance-identifier insurance-dw \
  --db-instance-class db.t4g.micro \
  --engine postgres \
  --master-username admin \
  --master-user-password <secure-password> \
  --allocated-storage 100

# 2. Deploy schema
python scripts/apply_ddl_to_rds.py

# 3. Verify deployment
python scripts/verify_dim_date.py
```

### Docker Deployment

```bash
# Build and push custom Airflow image
docker build -t your-registry/insurance-dw-airflow:latest .
docker push your-registry/insurance-dw-airflow:latest

# Deploy with Docker Compose or Kubernetes
docker-compose -f docker-compose.yml up -d
```

---

## 📝 Development Workflow

### Adding a New DAG

1. **Create DAG file** in `dags/` directory
2. **Define tasks** using Airflow SDK or decorators
3. **Configure schedule** (daily, weekly, monthly, etc.)
4. **Add quality rules** in `insurance_dw_common.py`
5. **Write integration tests** in `tests/`
6. **Test locally** with `pytest tests/`
7. **Deploy** to Airflow scheduler

### Example: New ETL DAG

```python
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

@dag(
    dag_id="my_new_etl",
    schedule="0 5 * * *",  # Daily at 5 AM UTC
    start_date="2024-01-01",
    catchup=False,
    tags=["etl"],
)
def my_new_etl():
    @task
    def extract():
        # Fetch data from API
        pass

    @task
    def transform(raw_data):
        # Transform and clean
        pass

    @task
    def load(clean_data):
        # Load to database
        pass

    load(transform(extract()))

dag_instance = my_new_etl()
```

---

## 🐛 Troubleshooting

### Common Issues

**Issue**: DAG not appearing in Airflow UI
```bash
# Solution: Check DAG parsing errors
airflow dags list-import-errors
airflow dags validate [dag_id]
```

**Issue**: Database connection timeout
```bash
# Solution: Verify connection string
export AIRFLOW_CONN_INSURANCE_RDS=postgresql://user:pass@host:5432/insurance_dw
airflow connections test insurance_rds
```

**Issue**: Tests failing with PostgreSQL error
```bash
# Solution: Ensure test database exists and is accessible
psql -h localhost -U postgres -c "CREATE DATABASE insurance_dw;" 2>/dev/null
python scripts/apply_ddl_to_rds.py --test
```

---

## 📚 Documentation

- **[DAG Test Suite](./DAG_TEST_SUITE.md)** - Comprehensive test documentation
- **[Test README](./tests/README.md)** - Test setup and execution guide
- **[Data Dictionary](./sql/ddl/create_schema.sql)** - Complete schema definition

---

## 🤝 Contributing

1. Create feature branch: `git checkout -b feature/new-dag`
2. Write integration tests first
3. Implement DAG logic
4. Run test suite: `./scripts/run_tests.sh`
5. Commit with clear message: `feat: add new ETL DAG for X insurance`
6. Push and create pull request

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 📞 Support

For questions or issues:
- Check the [test documentation](./tests/README.md)
- Review [DAG implementations](./dags/)
- Examine [test fixtures](./tests/conftest.py)

---

## 🗂️ File Statistics

```
Total Files: 50+
Python Files: 20+ (DAGs, tests, scripts)
SQL Files: 1 (Schema with 15 tables)
Test Coverage: 45 tests
Lines of Code: ~5,000+
Documentation: 3 markdown files
```

---

**Last Updated**: 2026-03-17
**Version**: 0.1.0
**Status**: ✅ Production Ready
