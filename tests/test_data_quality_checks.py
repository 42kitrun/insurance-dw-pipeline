"""Integration tests for data_quality_checks DAG."""
import pytest
from dags.data_quality_checks import insurance_data_quality_checks
from dags.insurance_dw_common import QUALITY_RULES


def test_quality_checks_dag_structure():
    """Verify quality checks DAG is properly defined."""
    dag = insurance_data_quality_checks()
    assert dag is not None
    assert dag.dag_id == "insurance_data_quality_checks"
    assert dag.schedule == "45 2 * * *"


def test_quality_rules_coverage(pg_hook):
    """Verify all fact tables have quality rules defined."""
    rule_tables = {rule.table_name for rule in QUALITY_RULES}

    expected_tables = {
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
    }

    assert rule_tables == expected_tables


def test_required_columns_null_check(pg_hook, clean_tables, populate_dim_date, sample_date_key):
    """Test NULL check for required columns."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert a row with NULL in required column
        cursor.execute("""
            INSERT INTO fact_auto_contract
            (date_key, ins_type, coverage, sex, age_group, origin, car_type, join_cnt, elapsed_premium)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (sample_date_key, None, "종합", "남", "30대", "국산", "승용차", 100, 5000))
        conn.commit()

        # Check should find the NULL
        cursor.execute("""
            SELECT COUNT(*) FROM fact_auto_contract WHERE ins_type IS NULL
        """)
        null_count = cursor.fetchone()[0]
        assert null_count == 1
    finally:
        cursor.close()
        conn.close()


def test_duplicate_detection(pg_hook, clean_tables, populate_dim_date, sample_date_key):
    """Test duplicate detection in fact tables."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert duplicate records
        for _ in range(2):
            cursor.execute("""
                INSERT INTO fact_auto_contract
                (date_key, ins_type, coverage, sex, age_group, origin, car_type, join_cnt, elapsed_premium)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (sample_date_key, "자동차", "종합", "남", "30대", "국산", "승용차", 100, 5000))
        conn.commit()

        # Query should find the duplicates
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT date_key, ins_type, coverage, sex, age_group, origin, car_type
                FROM fact_auto_contract
                GROUP BY date_key, ins_type, coverage, sex, age_group, origin, car_type
                HAVING COUNT(*) > 1
            ) duplicated
        """)
        duplicate_groups = cursor.fetchone()[0]
        assert duplicate_groups == 1
    finally:
        cursor.close()
        conn.close()


def test_life_general_quality_rules(pg_hook, clean_tables, populate_dim_date, sample_date_key):
    """Verify life_general fact table quality rules."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert valid record
        cursor.execute("""
            INSERT INTO fact_life_general
            (date_key, company_name, title, employee_type, person_cnt, employee_cnt)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sample_date_key, "회사A", "직원", "정규직", 100, 100))
        conn.commit()

        # Check required columns exist and are not null
        cursor.execute("""
            SELECT date_key, company_name, employee_type
            FROM fact_life_general
            WHERE date_key = %s
        """, (sample_date_key,))

        result = cursor.fetchone()
        assert result is not None
        assert result[0] == sample_date_key
        assert result[1] == "회사A"
        assert result[2] == "정규직"
    finally:
        cursor.close()
        conn.close()


def test_auto_loss_quality_rules(pg_hook, clean_tables, populate_dim_date, sample_date_key):
    """Verify auto_loss fact table quality rules."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert records to test duplicate detection
        for i in range(2):
            cursor.execute("""
                INSERT INTO fact_auto_loss
                (date_key, ins_type, coverage, car_type, loss_amount, injury_cnt, death_cnt)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (sample_date_key, "자동차", "종합", "승용차", 50000 + i, 2, 0))
        conn.commit()

        # Verify records inserted
        cursor.execute("""
            SELECT COUNT(*) FROM fact_auto_loss WHERE date_key = %s
        """, (sample_date_key,))

        count = cursor.fetchone()[0]
        assert count == 2
    finally:
        cursor.close()
        conn.close()


def test_nonlife_finance_quality_rules(pg_hook, clean_tables, populate_dim_date, sample_date_key):
    """Verify nonlife_finance fact table quality rules."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert records with unique duplicate keys
        cursor.execute("""
            INSERT INTO fact_nonlife_finance
            (date_key, company_name, account_code, account_name, amount, ratio)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (sample_date_key, "보험사A", "001", "총자산", 1000000, 100.0))
        conn.commit()

        # Verify insertion
        cursor.execute("""
            SELECT date_key, company_name, account_code
            FROM fact_nonlife_finance
            WHERE date_key = %s
        """, (sample_date_key,))

        result = cursor.fetchone()
        assert result is not None
    finally:
        cursor.close()
        conn.close()


def test_quality_check_with_valid_data(pg_hook, clean_tables, populate_dim_date, sample_date_key):
    """Test that quality checks pass with valid data."""
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    try:
        # Insert valid data
        cursor.execute("""
            INSERT INTO fact_auto_contract
            (date_key, ins_type, coverage, sex, age_group, origin, car_type, join_cnt, elapsed_premium)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (sample_date_key, "자동차", "종합", "남", "30대", "국산", "승용차", 100, 5000))
        conn.commit()

        # Run quality checks - should pass
        failures = []

        # Check required columns
        cursor.execute("""
            SELECT COUNT(*) FROM fact_auto_contract
            WHERE date_key IS NULL OR ins_type IS NULL OR coverage IS NULL
        """)
        if cursor.fetchone()[0] > 0:
            failures.append("NULL values in required columns")

        # Check duplicates
        cursor.execute("""
            SELECT COUNT(*) FROM (
                SELECT date_key, ins_type, coverage, sex, age_group, origin, car_type
                FROM fact_auto_contract
                GROUP BY date_key, ins_type, coverage, sex, age_group, origin, car_type
                HAVING COUNT(*) > 1
            ) duplicated
        """)
        if cursor.fetchone()[0] > 0:
            failures.append("Duplicate records found")

        assert len(failures) == 0, f"Quality checks failed: {failures}"
    finally:
        cursor.close()
        conn.close()
