"""Shared pytest fixtures for DAG integration tests."""
import os
import pytest
import psycopg2
from psycopg2.extensions import connection as pg_connection


class SimplePostgresHook:
    """Simple PostgreSQL connection wrapper for tests."""

    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    def get_conn(self):
        """Get database connection."""
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )


@pytest.fixture(scope="session")
def postgres_connection():
    """Create test PostgreSQL connection."""
    return SimplePostgresHook(
        host=os.getenv("TEST_DB_HOST", "localhost"),
        port=int(os.getenv("TEST_DB_PORT", 5432)),
        database=os.getenv("TEST_DB_NAME", "insurance_dw"),
        user=os.getenv("TEST_DB_USER", "postgres"),
        password=os.getenv("TEST_DB_PASSWORD", ""),
    )


@pytest.fixture
def pg_hook(postgres_connection):
    """Provide PostgreSQL hook for tests."""
    return postgres_connection


@pytest.fixture
def db_cursor(pg_hook):
    """Provide database cursor with cleanup."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    yield cursor
    cursor.close()
    conn.close()


@pytest.fixture
def clean_tables(db_cursor, pg_hook):
    """Clean fact tables before each test."""
    tables = [
        "fact_auto_contract",
        "fact_auto_loss",
        "fact_auto_victim",
        "fact_life_general",
        "fact_life_finance",
        "fact_life_business",
        "fact_life_kpi",
        "fact_nonlife_general",
        "fact_nonlife_finance",
        "fact_nonlife_business",
        "fact_nonlife_kpi",
        "mart_monthly_summary",
    ]
    conn = pg_hook.get_conn()
    try:
        for table in tables:
            conn.cursor().execute(f"TRUNCATE TABLE {table}")
        conn.commit()
    finally:
        conn.close()
    yield
    # Cleanup after test
    conn = pg_hook.get_conn()
    try:
        for table in tables:
            conn.cursor().execute(f"TRUNCATE TABLE {table}")
        conn.commit()
    finally:
        conn.close()


@pytest.fixture
def sample_date_key():
    """Provide a sample date_key in YYYYMMDD format."""
    return "20240101"


@pytest.fixture
def populate_dim_date(pg_hook):
    """Populate dim_date with test data."""
    from datetime import date, timedelta

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert date records for 2024 (for testing)
        start = date(2024, 1, 1)
        end = date(2024, 12, 31)

        records = []
        current = start
        while current <= end:
            date_key = current.strftime("%Y%m%d")
            year = current.year
            month = current.month
            day = current.day
            quarter = (month - 1) // 3 + 1
            week = current.isocalendar()[1]
            day_of_week = current.weekday() + 1  # 1=Monday
            is_weekend = day_of_week in (6, 7)
            records.append(
                (
                    date_key,
                    current,
                    year,
                    month,
                    day,
                    quarter,
                    week,
                    day_of_week,
                    is_weekend,
                )
            )
            current += timedelta(days=1)

        cursor.executemany(
            """
            INSERT INTO dim_date (
                date_key, full_date, year, month, day,
                quarter, week_of_year, day_of_week, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date_key) DO NOTHING
            """,
            records,
        )
        conn.commit()
        yield
    finally:
        cursor.close()
        conn.close()
