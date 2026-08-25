-- ==========================================
-- dbt Model: mart_user_retention
-- Cohort retention analysis mart
-- ==========================================

{{ config(
    materialized='table'
) }}

WITH user_first_activity AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', MIN(event_date)) AS cohort_month
    FROM {{ ref('silver_clickstream_events') }}
    GROUP BY user_id
),
user_monthly_activity AS (
    SELECT DISTINCT
        e.user_id,
        f.cohort_month,
        DATE_TRUNC('month', e.event_date) AS activity_month,
        EXTRACT(MONTH FROM AGE(DATE_TRUNC('month', e.event_date), f.cohort_month)) AS month_number
    FROM {{ ref('silver_clickstream_events') }} e
    JOIN user_first_activity f ON e.user_id = f.user_id
)
SELECT 
    cohort_month,
    month_number,
    COUNT(DISTINCT user_id) AS active_users,
    ROUND(COUNT(DISTINCT user_id)::NUMERIC / FIRST_VALUE(COUNT(DISTINCT user_id)) OVER (PARTITION BY cohort_month ORDER BY month_number) * 100, 2) AS retention_rate
FROM user_monthly_activity
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number
