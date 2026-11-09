-- ==============================================================================
-- [과제 06: Redshift 분석 쿼리 3종 및 전체 건수 검증]
-- 작성자: 데엔 7기 김준
-- 대상 워크그룹: de8-stock-wg (네임스페이스: de8-stock-ns, DB: dev)
-- ==============================================================================

-- 1) 종목별 평균종가·총거래량 (internal 테이블, GROUP BY)
SELECT 
    ticker,
    ROUND(AVG("close"), 2) AS avg_close,
    SUM(volume) AS total_volume
FROM stock_prices
GROUP BY ticker;

-- 2) stock_prices(internal) ⋈ stock_meta(external) 섹터별 평균 (Spectrum 조인)
SELECT 
    m.sector,
    ROUND(AVG(p."close"), 2) AS avg_close
FROM stock_prices p
JOIN spectrum_stock.stock_meta m ON p.ticker = m.ticker
GROUP BY m.sector;

-- 3) lag(close) over(partition by ticker order by trade_date) 전일대비 등락 (윈도우 함수)
SELECT 
    ticker,
    trade_date,
    "close",
    ROUND("close" - LAG("close", 1) OVER (PARTITION BY ticker ORDER BY trade_date), 2) AS change
FROM stock_prices
ORDER BY ticker, trade_date DESC;

-- 4) 위 3번(윈도우 함수) 결과 전체 건수 COUNT(*) 검증
WITH price_changes AS (
    SELECT 
        ticker,
        trade_date,
        "close",
        LAG("close", 1) OVER (PARTITION BY ticker ORDER BY trade_date) AS change
    FROM stock_prices
)
SELECT COUNT(*) AS total_rows FROM price_changes;
