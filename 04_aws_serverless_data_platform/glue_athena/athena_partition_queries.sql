-- ==========================================
-- Amazon Athena Cost-Optimized Partition Query
-- Uses Partition Projection & Columnar Parquet Scans
-- ==========================================

-- 1. Create External Table with Partition Projection
CREATE EXTERNAL TABLE IF NOT EXISTS analytics_catalog.user_clickstream (
    event_id STRING,
    user_id STRING,
    event_type STRING,
    page_url STRING,
    device STRING,
    event_timestamp TIMESTAMP
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://processed-analytics-lake/'
TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY',
    'projection.enabled' = 'true',
    'projection.year.type' = 'integer',
    'projection.year.range' = '2024,2027',
    'projection.month.type' = 'integer',
    'projection.month.range' = '1,12',
    'projection.month.digits' = '2',
    'projection.day.type' = 'integer',
    'projection.day.range' = '1,31',
    'projection.day.digits' = '2',
    'storage.location.template' = 's3://processed-analytics-lake/year=${year}/month=${month}/day=${day}'
);

-- 2. Cost-Optimized Analytical Query (Scans only 1 day partition instead of full lake)
SELECT 
    event_type,
    device,
    COUNT(DISTINCT user_id) as active_users,
    COUNT(*) as total_events
FROM analytics_catalog.user_clickstream
WHERE year = '2026' AND month = '08' AND day = '25'
GROUP BY event_type, device;
