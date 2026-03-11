from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import date, timedelta


@dag(
    dag_id="init_dim_date",
    schedule=None,
    start_date=None,
    catchup=False,
    tags=["init", "dim"],
)
def init_dim_date():

    @task
    def load_dim_date():
        hook = PostgresHook(postgres_conn_id="insurance_rds")
        conn = hook.get_conn()
        cursor = conn.cursor()

        start = date(2020, 1, 1)
        end = date(2030, 12, 31)

        records = []
        current = start
        while current <= end:
            date_key = int(current.strftime("%Y%m%d"))
            year = current.year
            month = current.month
            day = current.day
            quarter = (month - 1) // 3 + 1
            week = current.isocalendar()[1]
            day_of_week = current.weekday() + 1  # 1=Monday
            is_weekend = day_of_week in (6, 7)
            records.append((date_key, current, year, month, day, quarter, week, day_of_week, is_weekend))
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
        cursor.close()
        conn.close()
        print(f"dim_date 적재 완료: {len(records)}건")

    load_dim_date()


init_dim_date()
