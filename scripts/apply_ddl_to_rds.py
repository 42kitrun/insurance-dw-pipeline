#!/usr/bin/env python3
"""
RDS에 DDL 적용 스크립트
기존 테이블을 삭제하고 새로운 스키마를 생성합니다.
"""

import psycopg2
from pathlib import Path

# RDS 연결 정보
RDS_HOST = "3.38.85.243"
RDS_PORT = 5432
RDS_DBNAME = "postgres"
RDS_USER = "postgres"
RDS_PASSWORD = "insurance00"

# DDL 파일 경로
DDL_FILE = Path(__file__).parent.parent / "sql" / "ddl" / "create_schema.sql"


def main():
    """RDS에 DDL 적용"""
    try:
        # RDS 연결
        conn = psycopg2.connect(
            host=RDS_HOST,
            port=RDS_PORT,
            database=RDS_DBNAME,
            user=RDS_USER,
            password=RDS_PASSWORD,
        )
        cursor = conn.cursor()
        print(f"✓ RDS 연결 성공: {RDS_HOST}")

        # DDL 파일 읽기
        with open(DDL_FILE, "r", encoding="utf-8") as f:
            ddl_content = f.read()

        # 기존 테이블 삭제 (외래키 때문에 순서 중요)
        drop_tables = [
            "mart_monthly_summary",
            "fact_nonlife_kpi",
            "fact_nonlife_business",
            "fact_nonlife_finance",
            "fact_nonlife_general",
            "fact_life_kpi",
            "fact_life_business",
            "fact_life_finance",
            "fact_life_general",
            "fact_auto_victim",
            "fact_auto_loss",
            "fact_auto_contract",
            "dim_insurance_type",
            "dim_insurance_company",
            "dim_date",
        ]

        for table in drop_tables:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                print(f"✓ 테이블 삭제: {table}")
            except Exception as e:
                print(f"✗ 테이블 삭제 실패 {table}: {e}")

        conn.commit()

        # 새로운 DDL 실행
        cursor.execute(ddl_content)
        conn.commit()
        print(f"✓ DDL 적용 완료")

        # 테이블 확인
        cursor.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'dim_date'
            ORDER BY ordinal_position
            """
        )
        columns = cursor.fetchall()
        print("\n[dim_date 테이블 구조]")
        for table, col, dtype in columns:
            print(f"  {col}: {dtype}")

        cursor.close()
        conn.close()
        print("\n✓ 모든 작업 완료")

    except Exception as e:
        print(f"✗ 오류 발생: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
