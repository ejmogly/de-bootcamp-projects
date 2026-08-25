-- ==========================================
-- dbt Model: mart_ecommerce_funnel
-- Aggregates multi-stage conversion funnel by device & channel
-- ==========================================

{{ config(
    materialized='incremental',
    unique_key=['event_date', 'channel', 'device_type']
) }}

WITH silver_events AS (
    SELECT 
        event_date,
        session_id,
        user_id,
        channel,
        device_type,
        event_type
    FROM {{ ref('silver_clickstream_events') }}
    {% if is_incremental() %}
    WHERE event_date >= dateadd(day, -3, current_date())
    {% endif %}
),
session_funnel_flags AS (
    SELECT 
        event_date,
        channel,
        device_type,
        session_id,
        MAX(CASE WHEN event_type = 'view_item' THEN 1 ELSE 0 END) AS has_view,
        MAX(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS has_cart,
        MAX(CASE WHEN event_type = 'begin_checkout' THEN 1 ELSE 0 END) AS has_checkout,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchase
    FROM silver_events
    GROUP BY event_date, channel, device_type, session_id
)
SELECT 
    event_date,
    channel,
    device_type,
    COUNT(DISTINCT session_id) AS total_sessions,
    SUM(has_view) AS view_sessions,
    SUM(has_cart) AS cart_sessions,
    SUM(has_checkout) AS checkout_sessions,
    SUM(has_purchase) AS purchase_sessions,
    ROUND(SUM(has_cart)::NUMERIC / NULLIF(SUM(has_view), 0) * 100, 2) AS view_to_cart_cvr,
    ROUND(SUM(has_purchase)::NUMERIC / NULLIF(COUNT(DISTINCT session_id), 0) * 100, 2) AS overall_session_cvr
FROM session_funnel_flags
GROUP BY event_date, channel, device_type
