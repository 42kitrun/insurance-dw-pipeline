from airflow.sdk import dag, task
from pendulum import datetime

from insurance_dw_common import (
    ApiDatasetSpec,
    load_dataset,
    map_nonlife_business,
    map_nonlife_finance,
    map_nonlife_general,
    map_nonlife_kpi,
    required_endpoint,
)


@dag(
    dag_id="nonlife_insurance_etl",
    schedule="30 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "nonlife"],
)
def nonlife_insurance_etl():
    @task(task_id="load_general_status")
    def load_general_status() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "NONLIFE_GENERAL_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetNonlInsuCompInfoService/getNonlInsuCompGeneInfo",
                ),
                title="손보_일반현황_임직원및설계사현황",
                table_name="fact_nonlife_general",
                columns=("date_key", "company_name", "title", "employee_type", "person_cnt", "employee_cnt"),
                mapper=map_nonlife_general,
            )
        )

    @task(task_id="load_finance_status")
    def load_finance_status() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "NONLIFE_FINANCE_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetNonlInsuCompInfoService/getNonlInsuCompFinaInfo",
                ),
                title="손보_재무현황_요약재무상태표(자산-전체)",
                table_name="fact_nonlife_finance",
                columns=("date_key", "company_name", "account_code", "account_name", "amount", "ratio"),
                mapper=map_nonlife_finance,
            )
        )

    @task(task_id="load_business_activity")
    def load_business_activity() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint("NONLIFE_BUSINESS_API_ENDPOINT"),
                title=None,
                table_name="fact_nonlife_business",
                columns=("date_key", "company_name", "ins_type_code", "ins_type_name", "amount"),
                mapper=map_nonlife_business,
            )
        )

    @task(task_id="load_management_kpi")
    def load_management_kpi() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint("NONLIFE_KPI_API_ENDPOINT"),
                title=None,
                table_name="fact_nonlife_kpi",
                columns=("date_key", "company_name", "kpi_code", "kpi_name", "kpi_value"),
                mapper=map_nonlife_kpi,
            )
        )

    [load_general_status(), load_finance_status(), load_business_activity(), load_management_kpi()]


nonlife_insurance_etl()
