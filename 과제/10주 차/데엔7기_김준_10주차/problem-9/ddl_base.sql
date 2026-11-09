-- Problem 9: Base Table DDL without Key Optimization
-- 1. Base Orders Table (DISTSTYLE EVEN)
DROP TABLE IF EXISTS orders_base;
CREATE TABLE orders_base (
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    order_time TIMESTAMP NOT NULL,
    quantity INT NOT NULL
)
DISTSTYLE EVEN;

-- 2. Populate Base Orders Table from Hot Data
INSERT INTO orders_base (order_id, product_id, order_time, quantity)
SELECT order_id, product_id, order_time, quantity
FROM orders;

-- 3. Base Product Master Table (DISTSTYLE EVEN)
DROP TABLE IF EXISTS product_master_base;
CREATE TABLE product_master_base (
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
)
DISTSTYLE EVEN;

-- 4. Populate Base Product Master Table
INSERT INTO product_master_base (product_id, product_name, category, price)
SELECT product_id, product_name, category, price
FROM product_master;
