from __future__ import annotations

import os
import json
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import urlopen

from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import now, parse

POSTGRES_CONN_ID = "insurance_rds"
DATA_GO_KR_API_KEY_ENV = "DATA_GO_KR_API_KEY"
DEFAULT_API_TIMEOUT = 30
DEFAULT_PAGE_SIZE = 500


@dataclass(frozen=True)
class QualityRule:
    table_name: str
    required_columns: tuple[str, ...]
    duplicate_key_columns: tuple[str, ...]


@dataclass(frozen=True)
class ApiDatasetSpec:
    endpoint: str
    title: str | None
    table_name: str
    columns: tuple[str, ...]
    mapper: Callable[[dict, str], tuple]


QUALITY_RULES = (
    QualityRule(
        table_name="fact_auto_contract",
        required_columns=("date_key", "ins_type", "coverage"),
        duplicate_key_columns=("date_key", "ins_type", "coverage", "sex", "age_group", "origin", "car_type"),
    ),
    QualityRule(
        table_name="fact_auto_loss",
        required_columns=("date_key", "ins_type", "coverage", "car_type"),
        duplicate_key_columns=("date_key", "ins_type", "coverage", "car_type"),
    ),
    QualityRule(
        table_name="fact_auto_victim",
        required_columns=("date_key", "death_injury_type", "injury_level"),
        duplicate_key_columns=("date_key", "death_injury_type", "is_disabled", "injury_level", "disability_level"),
    ),
    QualityRule(
        table_name="fact_life_general",
        required_columns=("date_key", "company_name", "employee_type"),
        duplicate_key_columns=("date_key", "company_name", "title", "employee_type"),
    ),
    QualityRule(
        table_name="fact_life_finance",
        required_columns=("date_key", "company_name", "account_code"),
        duplicate_key_columns=("date_key", "company_name", "account_code"),
    ),
    QualityRule(
        table_name="fact_life_business",
        required_columns=("date_key", "company_name", "contract_type_code"),
        duplicate_key_columns=("date_key", "company_name", "contract_type_code"),
    ),
    QualityRule(
        table_name="fact_life_kpi",
        required_columns=("date_key", "company_name", "kpi_code"),
        duplicate_key_columns=("date_key", "company_name", "kpi_code"),
    ),
    QualityRule(
        table_name="fact_nonlife_general",
        required_columns=("date_key", "company_name", "employee_type"),
        duplicate_key_columns=("date_key", "company_name", "title", "employee_type"),
    ),
    QualityRule(
        table_name="fact_nonlife_finance",
        required_columns=("date_key", "company_name", "account_code"),
        duplicate_key_columns=("date_key", "company_name", "account_code"),
    ),
    QualityRule(
        table_name="fact_nonlife_business",
        required_columns=("date_key", "company_name", "ins_type_code"),
        duplicate_key_columns=("date_key", "company_name", "ins_type_code"),
    ),
    QualityRule(
        table_name="fact_nonlife_kpi",
        required_columns=("date_key", "company_name", "kpi_code"),
        duplicate_key_columns=("date_key", "company_name", "kpi_code"),
    ),
)


def postgres_hook() -> PostgresHook:
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def resolve_bas_ym(explicit_bas_ym: str | None = None) -> str:
    if explicit_bas_ym:
        return explicit_bas_ym
    configured = os.getenv("INSURANCE_API_BASYM")
    if configured:
        return configured
    return now("Asia/Seoul").subtract(months=2).format("YYYYMM")


def bas_ym_to_date_key(bas_ym: str) -> str:
    return parse(f"{bas_ym}01", tz="Asia/Seoul").end_of("month").format("YYYYMMDD")


def get_api_key() -> str:
    api_key = os.getenv(DATA_GO_KR_API_KEY_ENV)
    if not api_key:
        raise ValueError(f"{DATA_GO_KR_API_KEY_ENV} 환경변수가 없습니다.")
    return api_key


def required_endpoint(env_name: str, default: str | None = None) -> str:
    endpoint = os.getenv(env_name, default or "")
    if not endpoint:
        raise ValueError(f"{env_name} 환경변수가 없어 API 엔드포인트를 결정할 수 없습니다.")
    return endpoint


def fetch_table_items(endpoint: str, bas_ym: str, title: str | None = None) -> list[dict]:
    page_no = 1
    items: list[dict] = []
    while True:
        query = urlencode(
            {
                "serviceKey": get_api_key(),
                "pageNo": page_no,
                "numOfRows": DEFAULT_PAGE_SIZE,
                "resultType": "json",
                "basYm": bas_ym,
            }
        )
        request_url = f"{endpoint}?{query}"
        with urlopen(request_url, timeout=DEFAULT_API_TIMEOUT) as response:
            payload = json.loads(response.read().decode("utf-8"))

        body = payload["response"]["body"]
        table_list = body.get("tableList", [])
        matched_tables = [table for table in table_list if title is None or table.get("title") == title]
        if not matched_tables:
            available_titles = ", ".join(table.get("title", "") for table in table_list)
            raise ValueError(f"응답에서 title={title!r} 를 찾지 못했습니다. available={available_titles}")

        table = matched_tables[0]
        page_items = table.get("items", {}).get("item", [])
        if isinstance(page_items, dict):
            page_items = [page_items]
        items.extend(page_items)

        total_count = int(table.get("totalCount", 0))
        if len(items) >= total_count or not page_items:
            break
        page_no += 1
    return items


def replace_table_rows(table_name: str, columns: tuple[str, ...], rows: list[tuple]) -> int:
    hook = postgres_hook()
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        if rows:
            placeholders = ", ".join(["%s"] * len(columns))
            column_sql = ", ".join(columns)
            cursor.executemany(
                f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})",
                rows,
            )
        conn.commit()
        return len(rows)
    finally:
        cursor.close()
        conn.close()


def load_dataset(spec: ApiDatasetSpec, bas_ym: str | None = None) -> int:
    resolved_bas_ym = resolve_bas_ym(bas_ym)
    items = fetch_table_items(spec.endpoint, resolved_bas_ym, title=spec.title)
    rows = [spec.mapper(item, resolved_bas_ym) for item in items]
    return replace_table_rows(spec.table_name, spec.columns, rows)


def as_int(value: str | int | None) -> int:
    if value in (None, "", "-"):
        return 0
    return int(Decimal(str(value).replace(",", "")))


def as_numeric(value: str | float | None) -> Decimal:
    if value in (None, "", "-"):
        return Decimal("0")
    return Decimal(str(value).replace(",", ""))


def map_life_general(item: dict, bas_ym: str) -> tuple:
    count = as_int(item.get("xcsmPlnpnCnt"))
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        "생보_일반현황_임직원 및 설계사 현황",
        item.get("xcsmPlnpnDcdNm"),
        count,
        count,
    )


def map_life_finance(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        item.get("astSmryStfnpsAcitCd"),
        item.get("astSmryStfnpsAcitCdNm"),
        as_int(item.get("astSmryStfnpsAcitAmt")),
        as_numeric(item.get("astSmryStfnpsAcitCmpsRto")),
    )


def map_nonlife_general(item: dict, bas_ym: str) -> tuple:
    count = as_int(item.get("xcsmPlnpnCnt"))
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        "손보_일반현황_임직원및설계사현황",
        item.get("xcsmPlnpnDcdNm"),
        count,
        count,
    )


def map_nonlife_finance(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        item.get("astSmryStfnpsAcitCd"),
        item.get("astSmryStfnpsAcitCdNm"),
        as_int(item.get("astSmryStfnpsAcitAmt")),
        as_numeric(item.get("astSmryStfnpsAcitCmpsRto")),
    )


def map_auto_contract(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("insTypeNm"),
        item.get("covgNm"),
        item.get("sexNm"),
        item.get("ageNm"),
        item.get("domesticImportNm"),
        item.get("carTypeNm"),
        as_int(item.get("joinCnt")),
        as_int(item.get("elpsPrm")),
    )


def map_auto_loss(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("insTypeNm"),
        item.get("covgNm"),
        item.get("carTypeNm"),
        as_int(item.get("lossAmt")),
        as_int(item.get("injrCnt")),
        as_int(item.get("deathCnt")),
    )


def map_auto_victim(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("dthInjrTypeNm"),
        item.get("dablYn"),
        item.get("injrGrdNm"),
        item.get("dablGrdNm"),
        as_int(item.get("personCnt")),
    )


def map_life_business(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        item.get("newCtrDcd"),
        item.get("newCtrDcdNm"),
        as_int(item.get("thqtrAmt")),
        as_int(item.get("thqtrCnt")),
    )


def map_life_kpi(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        item.get("cptAdeqItemCd"),
        item.get("cptAdeqItemCdNm"),
        as_int(item.get("cptAdeqItemAmt")),
    )


def map_nonlife_business(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        item.get("insTypeLossRtDcd"),
        item.get("insTypeLossRtDcdNm"),
        as_int(item.get("amt")),
    )


def map_nonlife_kpi(item: dict, bas_ym: str) -> tuple:
    return (
        bas_ym_to_date_key(bas_ym),
        item.get("fncoNm"),
        item.get("kpiCd"),
        item.get("kpiCdNm"),
        str(item.get("kpiVal") or ""),
    )
