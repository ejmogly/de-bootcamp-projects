-- ==============================================================================
-- [과제 06: Gold 테이블 DDL 및 S3 Silver Parquet COPY 적재]
-- 작성자: 데엔 7기 김준
-- 대상 워크그룹: de8-stock-wg (네임스페이스: de8-stock-ns, DB: dev)
-- ==============================================================================

DROP TABLE IF EXISTS stock_prices;

CREATE TABLE stock_prices (
    ticker      VARCHAR(10) NOT NULL,
    trade_date  DATE NOT NULL,
    "open"      DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    "close"     DOUBLE PRECISION,
    volume      BIGINT,
    change_type VARCHAR(10),
    PRIMARY KEY (ticker, trade_date)
)
DISTSTYLE AUTO
SORTKEY (ticker, trade_date);

-- S3 Silver Parquet 데이터를 COPY 명령어로 적재 (FORMAT AS PARQUET 필수 준수)
COPY stock_prices
FROM 's3://realestate-junekim/stock/silver/'
IAM_ROLE 'arn:aws:iam::268790346668:role/de8-redshift-role'
FORMAT AS PARQUET;

-- 전체 적재 건수 검증 (제공 데이터 원본: 총 3,760행)
SELECT COUNT(*) AS total_rows FROM stock_prices;
