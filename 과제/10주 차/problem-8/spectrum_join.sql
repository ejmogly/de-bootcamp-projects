-- Problem 8: Redshift Spectrum Hot + Cold UNION ALL and Product Master JOIN
-- 1. Full Period Analysis Query: Category Revenue
WITH all_orders AS (
    SELECT order_id, product_id, order_time, quantity FROM orders
    UNION ALL
    SELECT order_id, product_id, order_time, quantity FROM spectrum_raw.orders_cold
)
SELECT
    p.category,
    ROUND(SUM(a.quantity * p.price), 2) AS revenue
FROM all_orders a
JOIN product_master p ON a.product_id = p.product_id
GROUP BY p.category
ORDER BY revenue DESC;

-- 2. Verification Query: Total Joined Rows (Expected: 4,994)
WITH all_orders AS (
    SELECT order_id, product_id, order_time, quantity FROM orders
    UNION ALL
    SELECT order_id, product_id, order_time, quantity FROM spectrum_raw.orders_cold
)
SELECT COUNT(*) AS total_joined_rows
FROM all_orders a
JOIN product_master p ON a.product_id = p.product_id;

-- 3. Verification Query: S3 Scan Proof from System View
SELECT
    query AS query_id,
    step AS step_name,
    trim(table_name) AS table_name,
    s3_scanned_rows AS input_rows,
    s3_scanned_bytes AS input_bytes
FROM SVL_S3QUERY_SUMMARY
ORDER BY query DESC
LIMIT 10;
