-- Problem 9: Benchmark Queries and EXPLAIN Plans
-- ========================================================
-- 1. Base Query EXPLAIN (Expects DS_BCAST_INNER / DS_DIST_INNER)
-- ========================================================
EXPLAIN
SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders_base o
JOIN product_master_base p ON o.product_id = p.product_id
WHERE o.order_time BETWEEN '2017-11-01' AND '2017-12-31'
GROUP BY p.category
ORDER BY total_quantity DESC;

-- ========================================================
-- 2. Optimized Query EXPLAIN (Expects DS_DIST_NONE - Colocated Join)
-- ========================================================
EXPLAIN
SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders_opt o
JOIN product_master_opt p ON o.product_id = p.product_id
WHERE o.order_time BETWEEN '2017-11-01' AND '2017-12-31'
GROUP BY p.category
ORDER BY total_quantity DESC;

-- ========================================================
-- 3. Result Equivalence Verification Queries
-- ========================================================
SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders_base o
JOIN product_master_base p ON o.product_id = p.product_id
WHERE o.order_time BETWEEN '2017-11-01' AND '2017-12-31'
GROUP BY p.category
ORDER BY p.category;

SELECT p.category, SUM(o.quantity) AS total_quantity
FROM orders_opt o
JOIN product_master_opt p ON o.product_id = p.product_id
WHERE o.order_time BETWEEN '2017-11-01' AND '2017-12-31'
GROUP BY p.category
ORDER BY p.category;
