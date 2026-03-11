#!/usr/bin/env python3
"""RDS의 dim_date 테이블 데이터 확인"""

import psycopg2

RDS_HOST = "3.38.85.243"
RDS_PORT = 5432
RDS_DBNAME = "postgres"
RDS_USER = "postgres"
RDS_PASSWORD = "insurance00"

conn = psycopg2.connect(
    host=RDS_HOST, port=RDS_PORT, database=RDS_DBNAME, user=RDS_USER, password=RDS_PASSWORD
)
cursor = conn.cursor()

# 전체 레코드 수
cursor.execute("SELECT COUNT(*) FROM dim_date")
total = cursor.fetchone()[0]
print(f"✓ 전체 레코드: {total}건")

# 샘플 데이터 (처음 5개)
cursor.execute(
    "SELECT date_key, full_date, year, month, day, quarter, week_of_year, day_of_week, is_weekend FROM dim_date ORDER BY date_key LIMIT 5"
)
print("\n[샘플 데이터 (처음 5개)]")
for row in cursor.fetchall():
    print(f"  {row}")

# 마지막 5개
cursor.execute(
    "SELECT date_key, full_date, year, month, day, quarter, week_of_year, day_of_week, is_weekend FROM dim_date ORDER BY date_key DESC LIMIT 5"
)
print("\n[샘플 데이터 (마지막 5개)]")
for row in reversed(cursor.fetchall()):
    print(f"  {row}")

cursor.close()
conn.close()
