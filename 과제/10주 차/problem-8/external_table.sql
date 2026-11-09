-- Problem 8: External Table Definition for Cold Data (2017/06 ~ 2017/10)
DROP TABLE IF EXISTS spectrum_raw.orders_cold;

CREATE EXTERNAL TABLE spectrum_raw.orders_cold (
    order_id VARCHAR(50),
    product_id VARCHAR(50),
    order_time TIMESTAMP,
    quantity INT
)
PARTITIONED BY (
    year VARCHAR(10),
    month VARCHAR(10)
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://de7-junekim-week10/topics/order-events/';

-- Register Cold Partitions (2,652 records total)
ALTER TABLE spectrum_raw.orders_cold ADD
  PARTITION (year='2017', month='06') LOCATION 's3://de7-junekim-week10/topics/order-events/year=2017/month=06/'
  PARTITION (year='2017', month='07') LOCATION 's3://de7-junekim-week10/topics/order-events/year=2017/month=07/'
  PARTITION (year='2017', month='08') LOCATION 's3://de7-junekim-week10/topics/order-events/year=2017/month=08/'
  PARTITION (year='2017', month='09') LOCATION 's3://de7-junekim-week10/topics/order-events/year=2017/month=09/'
  PARTITION (year='2017', month='10') LOCATION 's3://de7-junekim-week10/topics/order-events/year=2017/month=10/';
