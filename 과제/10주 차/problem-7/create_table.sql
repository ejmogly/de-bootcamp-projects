-- Problem 7: Redshift Serverless Table DDL
-- Orders Table (Hot Data)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    order_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    order_time TIMESTAMP NOT NULL,
    quantity INT NOT NULL
);

-- Product Master Table
DROP TABLE IF EXISTS product_master;
CREATE TABLE product_master (
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price NUMERIC(10, 2) NOT NULL
);
