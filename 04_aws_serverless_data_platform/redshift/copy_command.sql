-- ==========================================
-- Amazon Redshift High-Throughput COPY Command
-- Loads Parquet directly from S3 using IAM Role
-- ==========================================

COPY public.fact_user_events
FROM 's3://processed-analytics-lake/year=2026/month=08/'
IAM_ROLE 'arn:aws:iam::123456789012:role/RedshiftS3LoadRole'
FORMAT AS PARQUET
STATUPDATE ON;
