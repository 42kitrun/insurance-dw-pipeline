from airflow.sdk import dag, task
from pendulum import datetime

from insurance_dw_common import postgres_hook

MART_SQL = """
TRUNCATE TABLE mart_monthly_summary;

INSERT INTO mart_monthly_summary (
    date_key,
    company_name,
    company_type,
    total_employee_cnt,
    total_asset,
    new_contract_cnt
)
WITH employee_agg AS (
    SELECT date_key, company_name, '생명' AS company_type, SUM(employee_cnt) AS total_employee_cnt
    FROM fact_life_general
    GROUP BY date_key, company_name
    UNION ALL
    SELECT date_key, company_name, '손해' AS company_type, SUM(employee_cnt) AS total_employee_cnt
    FROM fact_nonlife_general
    GROUP BY date_key, company_name
),
asset_agg AS (
    SELECT date_key, company_name, MAX(amount) FILTER (WHERE account_name = '총자산') AS total_asset
    FROM fact_life_finance
    GROUP BY date_key, company_name
    UNION ALL
    SELECT date_key, company_name, MAX(amount) FILTER (WHERE account_name = '총자산') AS total_asset
    FROM fact_nonlife_finance
    GROUP BY date_key, company_name
),
contract_agg AS (
    SELECT date_key, company_name, SUM(quarter_cnt) AS new_contract_cnt
    FROM fact_life_business
    GROUP BY date_key, company_name
    UNION ALL
    SELECT date_key, company_name, SUM(amount) AS new_contract_cnt
    FROM fact_nonlife_business
    GROUP BY date_key, company_name
),
base AS (
    SELECT
        employee_agg.date_key,
        employee_agg.company_name,
        employee_agg.company_type,
        employee_agg.total_employee_cnt,
        COALESCE(asset_agg.total_asset, 0) AS total_asset,
        COALESCE(contract_agg.new_contract_cnt, 0) AS new_contract_cnt
    FROM employee_agg
    LEFT JOIN asset_agg
      ON employee_agg.date_key = asset_agg.date_key
     AND employee_agg.company_name = asset_agg.company_name
    LEFT JOIN contract_agg
      ON employee_agg.date_key = contract_agg.date_key
     AND employee_agg.company_name = contract_agg.company_name
)
SELECT
    date_key,
    company_name,
    company_type,
    total_employee_cnt,
    total_asset,
    new_contract_cnt
FROM base;
"""


@dag(
    dag_id="insurance_monthly_summary_mart",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["mart"],
)
def insurance_monthly_summary_mart():
    @task
    def build_monthly_summary() -> None:
        hook = postgres_hook()
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(MART_SQL)
            conn.commit()
        finally:
            cursor.close()
            conn.close()

    build_monthly_summary()


insurance_monthly_summary_mart()
