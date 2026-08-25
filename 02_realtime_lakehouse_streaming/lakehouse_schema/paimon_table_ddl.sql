-- ==========================================
-- Apache Paimon (Changelog Lakehouse) Table DDL
-- Supports Primary Key Upsert / Delete in Real-time
-- ==========================================

CREATE TABLE IF NOT EXISTS paimon.ecommerce.orders_changelog (
    order_id STRING,
    user_id STRING,
    item_id STRING,
    event_type STRING,
    order_amount DOUBLE,
    updated_at TIMESTAMP,
    payment_method STRING,
    region STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
)
WITH (
    'bucket' = '4',
    'changelog-producer' = 'lookup',
    'merge-engine' = 'deduplicate'
);
