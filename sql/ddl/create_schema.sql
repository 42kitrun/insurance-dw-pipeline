-- ============================================
-- Insurance DW Schema DDL
-- ============================================

-- Dimension: 기준년월
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        CHAR(6) PRIMARY KEY,  -- YYYYMM
    year            INT NOT NULL,
    month           INT NOT NULL,
    quarter         INT NOT NULL,
    year_month_desc VARCHAR(20)
);

-- Dimension: 보험회사
CREATE TABLE IF NOT EXISTS dim_insurance_company (
    company_id      SERIAL PRIMARY KEY,
    company_name    VARCHAR(100) NOT NULL UNIQUE,
    company_type    VARCHAR(20),  -- 생명보험, 손해보험
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: 보험종목
CREATE TABLE IF NOT EXISTS dim_insurance_type (
    type_id         SERIAL PRIMARY KEY,
    type_name       VARCHAR(100) NOT NULL,  -- 개인용, 영업용 등
    type_category   VARCHAR(50),            -- 자동차, 생명, 손해
    UNIQUE (type_name, type_category)
);

-- ============================================
-- Fact: 자동차보험 계약정보
-- ============================================
CREATE TABLE IF NOT EXISTS fact_auto_contract (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    ins_type        VARCHAR(50),   -- 보험종목명 (개인용/영업용/업무용)
    coverage        VARCHAR(50),   -- 담보구분명
    sex             VARCHAR(10),   -- 성별
    age_group       VARCHAR(20),   -- 연령
    origin          VARCHAR(20),   -- 국산/외산
    car_type        VARCHAR(20),   -- 차종
    join_cnt        BIGINT,        -- 가입대수
    elapsed_premium BIGINT,        -- 경과보험료
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 자동차보험 손해상황
-- ============================================
CREATE TABLE IF NOT EXISTS fact_auto_loss (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    ins_type        VARCHAR(50),   -- 보험종목명
    coverage        VARCHAR(50),   -- 담보구분명
    car_type        VARCHAR(20),   -- 차종
    loss_amount     BIGINT,        -- 손해액
    injury_cnt      BIGINT,        -- 부상분손 건수
    death_cnt       BIGINT,        -- 사망전손 건수
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 자동차사고 피해자정보
-- ============================================
CREATE TABLE IF NOT EXISTS fact_auto_victim (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    death_injury_type VARCHAR(10), -- 사망/부상 구분
    is_disabled     VARCHAR(1),    -- 장애여부
    injury_level    VARCHAR(10),   -- 부상급수
    disability_level VARCHAR(10),  -- 장애급수
    person_cnt      BIGINT,        -- 인원수
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 생명보험 일반현황
-- ============================================
CREATE TABLE IF NOT EXISTS fact_life_general (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    title           VARCHAR(200),
    employee_type   VARCHAR(50),   -- 임직원 구분코드명
    person_cnt      BIGINT,        -- 인원수
    employee_cnt    BIGINT,        -- 임직원수
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 생명보험 재무현황
-- ============================================
CREATE TABLE IF NOT EXISTS fact_life_finance (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    account_code    VARCHAR(50),   -- 계정과목코드
    account_name    VARCHAR(200),  -- 계정과목코드명
    amount          BIGINT,        -- 금액
    ratio           NUMERIC(10,4), -- 구성비율
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 생명보험 주요영업활동
-- ============================================
CREATE TABLE IF NOT EXISTS fact_life_business (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    contract_type_code VARCHAR(50),   -- 신계약구분코드
    contract_type_name VARCHAR(200),  -- 신계약구분코드명
    quarter_amount  BIGINT,           -- 당분기금액
    quarter_cnt     BIGINT,           -- 당분기건수
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 생명보험 주요경영지표
-- ============================================
CREATE TABLE IF NOT EXISTS fact_life_kpi (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    kpi_code        VARCHAR(50),   -- 자본적정성항목코드
    kpi_name        VARCHAR(200),  -- 자본적정성항목코드명
    kpi_amount      BIGINT,        -- 금액
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 손해보험 일반현황
-- ============================================
CREATE TABLE IF NOT EXISTS fact_nonlife_general (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    title           VARCHAR(200),
    employee_type   VARCHAR(50),
    person_cnt      BIGINT,
    employee_cnt    BIGINT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 손해보험 재무현황
-- ============================================
CREATE TABLE IF NOT EXISTS fact_nonlife_finance (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    account_code    VARCHAR(50),
    account_name    VARCHAR(200),
    amount          BIGINT,
    ratio           NUMERIC(10,4),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 손해보험 주요영업활동
-- ============================================
CREATE TABLE IF NOT EXISTS fact_nonlife_business (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    ins_type_code   VARCHAR(50),   -- 보험종류경과손해율구분코드
    ins_type_name   VARCHAR(200),  -- 보험종류경과손해율구분코드명
    amount          BIGINT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Fact: 손해보험 주요경영지표
-- ============================================
CREATE TABLE IF NOT EXISTS fact_nonlife_kpi (
    id              SERIAL PRIMARY KEY,
    date_key        CHAR(6) REFERENCES dim_date(date_key),
    company_name    VARCHAR(100),
    kpi_code        VARCHAR(50),
    kpi_name        VARCHAR(200),
    kpi_value       TEXT,          -- 값이 숫자/문자 혼재
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Mart: 월별 보험사 통합 요약
-- ============================================
CREATE TABLE IF NOT EXISTS mart_monthly_summary (
    id                  SERIAL PRIMARY KEY,
    date_key            CHAR(6) REFERENCES dim_date(date_key),
    company_name        VARCHAR(100),
    company_type        VARCHAR(20),   -- 생명/손해
    total_employee_cnt  BIGINT,        -- 임직원수 합계
    total_asset         BIGINT,        -- 총자산
    new_contract_cnt    BIGINT,        -- 신계약건수
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- Index
-- ============================================
CREATE INDEX IF NOT EXISTS idx_auto_contract_date ON fact_auto_contract(date_key);
CREATE INDEX IF NOT EXISTS idx_auto_loss_date ON fact_auto_loss(date_key);
CREATE INDEX IF NOT EXISTS idx_auto_victim_date ON fact_auto_victim(date_key);
CREATE INDEX IF NOT EXISTS idx_life_general_date ON fact_life_general(date_key);
CREATE INDEX IF NOT EXISTS idx_life_finance_date ON fact_life_finance(date_key);
CREATE INDEX IF NOT EXISTS idx_nonlife_general_date ON fact_nonlife_general(date_key);
CREATE INDEX IF NOT EXISTS idx_nonlife_finance_date ON fact_nonlife_finance(date_key);
CREATE INDEX IF NOT EXISTS idx_mart_summary_date ON mart_monthly_summary(date_key);
