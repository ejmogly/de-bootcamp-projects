-- ==========================================
-- 01. Advanced SQL & Data Warehouse Modeling
-- Topics: Window Functions, CTE, Deduplication, SCD Type 2
-- ==========================================

-- 1. Deduplication using ROW_NUMBER()
WITH ranked_events AS (
    SELECT 
        event_id,
        user_id,
        event_timestamp,
        payload,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_timestamp DESC) as rank_idx
    FROM raw_event_stream
)
SELECT * FROM ranked_events WHERE rank_idx = 1;

-- 2. Slowly Changing Dimension (SCD Type 2) Tracking
CREATE TABLE IF NOT EXISTS dim_seller_scd2 (
    seller_sk SERIAL PRIMARY KEY,
    seller_id VARCHAR(64) NOT NULL,
    seller_tier VARCHAR(16) NOT NULL,
    commission_rate NUMERIC(5, 2),
    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP DEFAULT '9999-12-31 23:59:59',
    is_current BOOLEAN DEFAULT TRUE
);
