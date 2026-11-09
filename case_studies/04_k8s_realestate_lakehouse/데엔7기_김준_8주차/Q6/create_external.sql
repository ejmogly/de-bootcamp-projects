-- ==============================================================================
-- [과제 06: Redshift Spectrum External Schema 및 External Table 생성]
-- 작성자: 데엔 7기 김준
-- 대상 워크그룹: de8-stock-wg (네임스페이스: de8-stock-ns, DB: dev)
-- ==============================================================================

-- 1. AWS Glue Data Catalog 기반 외부 스키마 생성 (spectrum_stock)
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_stock
FROM DATA CATALOG
DATABASE 'spectrum_db'
IAM_ROLE 'arn:aws:iam::268790346668:role/de8-redshift-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- 2. S3 메타데이터 CSV를 직접 쿼리하는 외부 테이블 생성 (stock_meta)
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_stock.stock_meta (
    ticker  VARCHAR(10),
    name    VARCHAR(100),
    sector  VARCHAR(50)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://realestate-junekim/stock/meta/'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- ==============================================================================
-- [문제지 예시(6-2)와 100% 동일한 4단계 검증 쿼리]
-- ==============================================================================

-- 1) 로그인 확인
SELECT current_user;

-- 2) 원본 테이블 건수 확인
SELECT COUNT(*) FROM stock_prices;

-- 3) Spectrum external schema 확인
SELECT * FROM svv_external_schemas;

-- 4) Spectrum external table 확인 (6-2 검증 대상)
SELECT * FROM svv_external_tables;
