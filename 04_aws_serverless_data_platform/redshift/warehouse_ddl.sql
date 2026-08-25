-- ==========================================
-- Amazon Redshift Star Schema DDL
-- Optimized with DISTKEY (User ID) & SORTKEY (Date)
-- ==========================================

CREATE TABLE IF NOT EXISTS public.fact_user_events (
    event_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL DISTKEY,
    event_timestamp TIMESTAMP NOT NULL SORTKEY,
    event_type VARCHAR(32) NOT NULL,
    page_url VARCHAR(256),
    device_type VARCHAR(16),
    session_id VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS public.dim_users (
    user_id VARCHAR(64) NOT NULL DISTKEY,
    signup_date DATE NOT NULL,
    user_tier VARCHAR(16),
    region VARCHAR(32)
);
