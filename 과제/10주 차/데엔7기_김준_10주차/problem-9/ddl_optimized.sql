-- Problem 9: Optimized Table DDL with DISTKEY and SORTKEY
-- 1. Optimized Product Master Table (DISTKEY product_id)
DROP TABLE IF EXISTS product_master_opt;
CREATE TABLE product_master_opt (
    product_id VARCHAR(50) NOT NULL DISTKEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);

-- 2. Populate Optimized Product Master Table
INSERT INTO product_master_opt (product_id, product_name, category, price)
SELECT product_id, product_name, category, price
FROM product_master;

-- 3. Optimized Orders Table (DISTKEY product_id, SORTKEY order_time)
DROP TABLE IF EXISTS orders_opt;
CREATE TABLE orders_opt (
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL DISTKEY,
    order_time TIMESTAMP NOT NULL SORTKEY,
    quantity INT NOT NULL
);

-- 4. Populate Optimized Orders Table from Hot Data
INSERT INTO orders_opt (order_id, product_id, order_time, quantity)
SELECT order_id, product_id, order_time, quantity
FROM orders;
