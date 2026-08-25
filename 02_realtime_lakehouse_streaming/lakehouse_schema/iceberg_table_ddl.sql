-- ==========================================
-- Apache Iceberg Table DDL
-- Real-time Streaming Lakehouse Table
-- ==========================================

CREATE TABLE IF NOT EXISTS lakehouse.ecommerce.orders_realtime (
    order_id STRING,
    user_id STRING,
    item_id STRING,
    event_type STRING,
    order_amount DOUBLE,
    event_timestamp TIMESTAMP,
    payment_method STRING,
    region STRING,
    dt DATE
)
USING iceberg
PARTITIONED BY (days(event_timestamp), region)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'zstd',
    'history.expire.max-snapshot-age-ms' = '604800000', -- 7 days snapshot retention
    'write.merge.mode' = 'merge-on-read',
    'write.upsert.enabled' = 'true'
);

-- Optimization & Maintenance Query (Compaction)
-- CALL lakehouse.system.rewrite_data_files('lakehouse.ecommerce.orders_realtime');
