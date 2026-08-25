-- ==========================================
-- Lambda Architecture Unified Serving View
-- Merges Speed Layer (Real-time) + Batch Layer (Historical)
-- ==========================================

CREATE OR REPLACE VIEW serving.v_unified_search_keyword_trend AS
WITH historical_batch AS (
    SELECT 
        date_id::TIMESTAMP as event_time,
        search_keyword,
        SUM(search_count) as total_searches,
        'BATCH' as data_source
    FROM serving.fact_daily_keyword_summary
    WHERE date_id < CURRENT_DATE
    GROUP BY date_id, search_keyword
),
realtime_speed AS (
    SELECT 
        window_start as event_time,
        search_keyword,
        search_count as total_searches,
        'REALTIME' as data_source
    FROM serving.realtime_trending_keywords
    WHERE window_start >= CURRENT_DATE
)
SELECT * FROM historical_batch
UNION ALL
SELECT * FROM realtime_speed;
