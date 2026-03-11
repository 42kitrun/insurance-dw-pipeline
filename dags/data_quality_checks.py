from airflow.sdk import dag, task
from pendulum import datetime

from insurance_dw_common import QUALITY_RULES, postgres_hook


@dag(
    dag_id="insurance_data_quality_checks",
    schedule="45 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["quality"],
)
def insurance_data_quality_checks():
    @task
    def check_required_columns() -> None:
        hook = postgres_hook()
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            failures: list[str] = []
            for rule in QUALITY_RULES:
                for column_name in rule.required_columns:
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {rule.table_name} WHERE {column_name} IS NULL"
                    )
                    null_count = cursor.fetchone()[0]
                    if null_count:
                        failures.append(f"{rule.table_name}.{column_name} null={null_count}")
            if failures:
                raise ValueError("NULL check failed: " + ", ".join(failures))
        finally:
            cursor.close()
            conn.close()

    @task
    def check_duplicates() -> None:
        hook = postgres_hook()
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            failures: list[str] = []
            for rule in QUALITY_RULES:
                group_columns = ", ".join(rule.duplicate_key_columns)
                cursor.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM (
                        SELECT {group_columns}
                        FROM {rule.table_name}
                        GROUP BY {group_columns}
                        HAVING COUNT(*) > 1
                    ) duplicated
                    """
                )
                duplicate_groups = cursor.fetchone()[0]
                if duplicate_groups:
                    failures.append(f"{rule.table_name} duplicate_groups={duplicate_groups}")
            if failures:
                raise ValueError("Duplicate check failed: " + ", ".join(failures))
        finally:
            cursor.close()
            conn.close()

    check_required_columns() >> check_duplicates()


insurance_data_quality_checks()
