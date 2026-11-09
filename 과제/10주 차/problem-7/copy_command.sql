-- Problem 7: COPY Commands for Hot Data (Recent 3 Months) and Product Master
-- 1. Load Hot Orders for 2017/11 (1,183 records)
COPY orders
FROM 's3://de7-junekim-week10/topics/order-events/year=2017/month=11/'
IAM_ROLE 'arn:aws:iam::268790346668:role/redshift-copy-role'
FORMAT AS JSON 'auto'
TIMEFORMAT 'auto';

-- 2. Load Hot Orders for 2017/12 (893 records)
COPY orders
FROM 's3://de7-junekim-week10/topics/order-events/year=2017/month=12/'
IAM_ROLE 'arn:aws:iam::268790346668:role/redshift-copy-role'
FORMAT AS JSON 'auto'
TIMEFORMAT 'auto';

-- 3. Load Hot Orders for 2018/01 (321 records)
COPY orders
FROM 's3://de7-junekim-week10/topics/order-events/year=2018/month=01/'
IAM_ROLE 'arn:aws:iam::268790346668:role/redshift-copy-role'
FORMAT AS JSON 'auto'
TIMEFORMAT 'auto';

-- 4. Load Product Master (90 records)
COPY product_master
FROM 's3://de7-junekim-week10/data/product_master.csv'
IAM_ROLE 'arn:aws:iam::268790346668:role/redshift-copy-role'
CSV
IGNOREHEADER 1;

-- Verification Queries
SELECT COUNT(*) AS orders_count FROM orders;
SELECT COUNT(*) AS product_master_count FROM product_master;
