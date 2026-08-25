-- ==========================================
-- Serving Layer: PostgreSQL & Data Mart Schemas
-- ==========================================

CREATE SCHEMA IF NOT EXISTS serving;

-- 1. Batch Table: Daily Document Performance
CREATE TABLE IF NOT EXISTS serving.fact_daily_document_metrics (
    date_id DATE NOT NULL,
    document_id VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    unique_viewers INT DEFAULT 0,
    total_interactions INT DEFAULT 0,
    pdf_downloads INT DEFAULT 0,
    bookmarks INT DEFAULT 0,
    avg_duration_sec NUMERIC(10, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date_id, document_id)
);

CREATE INDEX IF NOT EXISTS idx_doc_metrics_cat ON serving.fact_daily_document_metrics(category, date_id);

-- 2. Speed Table: Real-time Trending Window (Updated by Spark Streaming)
CREATE TABLE IF NOT EXISTS serving.realtime_trending_keywords (
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    search_keyword VARCHAR(128) NOT NULL,
    search_count INT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, search_keyword)
);

CREATE INDEX IF NOT EXISTS idx_rt_trending ON serving.realtime_trending_keywords(window_end DESC, search_count DESC);
