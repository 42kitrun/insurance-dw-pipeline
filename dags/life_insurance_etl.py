from airflow.sdk import dag, task
from pendulum import datetime

from insurance_dw_common import (
    ApiDatasetSpec,
    load_dataset,
    map_life_business,
    map_life_finance,
    map_life_general,
    map_life_kpi,
    required_endpoint,
)


@dag(
    dag_id="life_insurance_etl",
    schedule="15 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "life"],
)
def life_insurance_etl():
    @task(task_id="load_general_status")
    def load_general_status() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "LIFE_GENERAL_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetLifeInsuCompInfoService/getLifeInsuCompGeneInfo",
                ),
                title="생보_일반현황_임직원 및 설계사 현황",
                table_name="fact_life_general",
                columns=("date_key", "company_name", "title", "employee_type", "person_cnt", "employee_cnt"),
                mapper=map_life_general,
            )
        )

    @task(task_id="load_finance_status")
    def load_finance_status() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "LIFE_FINANCE_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetLifeInsuCompInfoService/getLifeInsuCompFinaInfo",
                ),
                title="생보_재무현황_요약재무상태표(자산-전체)",
                table_name="fact_life_finance",
                columns=("date_key", "company_name", "account_code", "account_name", "amount", "ratio"),
                mapper=map_life_finance,
            )
        )

    @task(task_id="load_business_activity")
    def load_business_activity() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint("LIFE_BUSINESS_API_ENDPOINT"),
                title=None,
                table_name="fact_life_business",
                columns=(
                    "date_key",
                    "company_name",
                    "contract_type_code",
                    "contract_type_name",
                    "quarter_amount",
                    "quarter_cnt",
                ),
                mapper=map_life_business,
            )
        )

    @task(task_id="load_management_kpi")
    def load_management_kpi() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint("LIFE_KPI_API_ENDPOINT"),
                title=None,
                table_name="fact_life_kpi",
                columns=("date_key", "company_name", "kpi_code", "kpi_name", "kpi_amount"),
                mapper=map_life_kpi,
            )
        )

    [load_general_status(), load_finance_status(), load_business_activity(), load_management_kpi()]


life_insurance_etl()
