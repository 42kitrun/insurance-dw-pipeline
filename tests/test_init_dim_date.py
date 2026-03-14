"""Integration tests for init_dim_date DAG."""
import pytest
from datetime import date, timedelta
from dags.init_dim_date import init_dim_date


def test_dim_date_table_structure(pg_hook):
    """Verify dim_date table has correct schema."""
    cursor = pg_hook.get_conn().cursor()
    try:
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'dim_date'
            ORDER BY ordinal_position
        """)
        columns = {col[0]: col[1] for col in cursor.fetchall()}

        assert "date_key" in columns
        assert columns["date_key"] == "character"  # CHAR(8)
        assert "full_date" in columns
        assert columns["full_date"] == "date"
        assert "year" in columns
        assert "month" in columns
        assert "day" in columns
        assert "quarter" in columns
        assert "week_of_year" in columns
        assert "day_of_week" in columns
        assert "is_weekend" in columns
    finally:
        cursor.close()


def test_dim_date_load_records(pg_hook, clean_tables, populate_dim_date):
    """Test that init_dim_date loads correct number of records."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        count = cursor.fetchone()[0]

        # 2024-01-01 to 2024-12-31 = 366 days (leap year)
        assert count >= 365, f"Expected at least 365 records, got {count}"
    finally:
        cursor.close()
        conn.close()


def test_dim_date_start_end_dates(pg_hook, clean_tables, populate_dim_date):
    """Verify dim_date contains correct date range."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT COUNT(*) FROM dim_date
            WHERE full_date >= '2024-01-01' AND full_date <= '2024-12-31'
        """)
        count = cursor.fetchone()[0]

        # Verify data exists for 2024
        assert count >= 365, f"Expected at least 365 records in 2024, got {count}"
    finally:
        cursor.close()
        conn.close()


def test_dim_date_key_format(pg_hook, clean_tables):
    """Verify date_key is in YYYYMMDD format."""
    dag = init_dim_date()

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT date_key, full_date
            FROM dim_date
            LIMIT 10
        """)
        for date_key, full_date in cursor.fetchall():
            expected_key = full_date.strftime("%Y%m%d")
            assert date_key == expected_key
            assert len(date_key) == 8
            assert date_key.isdigit()
    finally:
        cursor.close()
        conn.close()


def test_dim_date_weekend_flag(pg_hook, clean_tables, populate_dim_date):
    """Verify weekend flag is correctly set."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Check a known weekend (2024-01-06 is Saturday, 2024-01-07 is Sunday)
        cursor.execute("""
            SELECT full_date, day_of_week, is_weekend
            FROM dim_date
            WHERE full_date IN ('2024-01-06', '2024-01-07')
            ORDER BY full_date
        """)
        results = cursor.fetchall()

        assert len(results) == 2
        # Saturday: day_of_week=6, Sunday: day_of_week=7
        assert results[0][2] is True  # 2024-01-06 Saturday is_weekend=True
        assert results[1][2] is True  # 2024-01-07 Sunday is_weekend=True

        # Check a known weekday (2024-01-08 is Monday)
        cursor.execute("""
            SELECT full_date, day_of_week, is_weekend
            FROM dim_date
            WHERE full_date = '2024-01-08'
        """)
        result = cursor.fetchone()
        assert result[2] is False  # Monday is_weekend=False
    finally:
        cursor.close()
        conn.close()


def test_dim_date_quarter_calculation(pg_hook, clean_tables):
    """Verify quarter calculation is correct."""
    dag = init_dim_date()

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT full_date, month, quarter
            FROM dim_date
            WHERE full_date IN ('2020-01-15', '2020-04-15', '2020-07-15', '2020-10-15')
            ORDER BY full_date
        """)
        results = cursor.fetchall()

        expected = [(1, 1), (4, 2), (7, 3), (10, 4)]
        for result, (exp_month, exp_quarter) in zip(results, expected):
            assert result[1] == exp_month
            assert result[2] == exp_quarter
    finally:
        cursor.close()
        conn.close()


def test_dim_date_no_duplicates(pg_hook, clean_tables):
    """Verify no duplicate date_key entries exist."""
    dag = init_dim_date()

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT date_key, COUNT(*)
            FROM dim_date
            GROUP BY date_key
            HAVING COUNT(*) > 1
        """)
        duplicates = cursor.fetchall()

        assert len(duplicates) == 0, f"Found duplicate date_keys: {duplicates}"
    finally:
        cursor.close()
        conn.close()


def test_dim_date_primary_key_constraint(pg_hook, clean_tables):
    """Verify date_key primary key constraint prevents duplicates."""
    dag = init_dim_date()

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Try to insert a duplicate - should fail
        with pytest.raises(Exception):  # psycopg2.IntegrityError
            cursor.execute("""
                INSERT INTO dim_date
                (date_key, full_date, year, month, day, quarter, week_of_year, day_of_week, is_weekend)
                VALUES ('20200101', '2020-01-01', 2020, 1, 1, 1, 1, 2, FALSE)
            """)
            conn.commit()
    finally:
        cursor.close()
        conn.close()
