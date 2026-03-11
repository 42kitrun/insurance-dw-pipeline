from airflow.sdk import dag, task
from pendulum import datetime

from insurance_dw_common import (
    ApiDatasetSpec,
    load_dataset,
    map_auto_contract,
    map_auto_loss,
    map_auto_victim,
    required_endpoint,
)


@dag(
    dag_id="auto_insurance_etl",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "auto"],
)
def auto_insurance_etl():
    @task(task_id="load_contract_info")
    def load_contract_info() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "AUTO_CONTRACT_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetFPAtmbInsujoinInfoService/getContractInfo",
                ),
                title=None,
                table_name="fact_auto_contract",
                columns=(
                    "date_key",
                    "ins_type",
                    "coverage",
                    "sex",
                    "age_group",
                    "origin",
                    "car_type",
                    "join_cnt",
                    "elapsed_premium",
                ),
                mapper=map_auto_contract,
                extra_params={"likeIsuCmpyOfrYm": "{bas_ym}"},
            )
        )

    @task(task_id="load_loss_status")
    def load_loss_status() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "AUTO_LOSS_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetFPAtmbInsujoinInfoService/getLosCircumstance",
                ),
                title=None,
                table_name="fact_auto_loss",
                columns=(
                    "date_key",
                    "ins_type",
                    "coverage",
                    "car_type",
                    "loss_amount",
                    "injury_cnt",
                    "death_cnt",
                ),
                mapper=map_auto_loss,
                extra_params={"likeIsuCmpyOfrYm": "{bas_ym}"},
            )
        )

    @task(task_id="load_victim_info")
    def load_victim_info() -> int:
        return load_dataset(
            ApiDatasetSpec(
                endpoint=required_endpoint(
                    "AUTO_VICTIM_API_ENDPOINT",
                    "http://apis.data.go.kr/1160100/service/GetFPAtmbInsujoinInfoService/getVictimInfo",
                ),
                title=None,
                table_name="fact_auto_victim",
                columns=(
                    "date_key",
                    "death_injury_type",
                    "is_disabled",
                    "injury_level",
                    "disability_level",
                    "person_cnt",
                ),
                mapper=map_auto_victim,
                extra_params={"likeAtmbAcdnCnlsYm": "{bas_ym}"},
            )
        )

    [load_contract_info(), load_loss_status(), load_victim_info()]


auto_insurance_etl()
