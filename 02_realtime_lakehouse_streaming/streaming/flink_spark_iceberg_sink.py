import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SCHEMA = StructType([
    StructField('order_id', StringType(), False),
    StructField('user_id', StringType(), False),
    StructField('item_id', StringType(), False),
    StructField('event_type', StringType(), False),
    StructField('order_amount', DoubleType(), True),
    StructField('event_timestamp', StringType(), False),
    StructField('payment_method', StringType(), True),
    StructField('region', StringType(), True)
])

def create_iceberg_spark():
    return SparkSession.builder \
        .appName('KafkaToIcebergLakehouseStreaming') \
        .config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1') \
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.lakehouse.type', 'hadoop') \
        .config('spark.sql.catalog.lakehouse.warehouse', 's3a://lakehouse-warehouse/') \
        .getOrCreate()

def main():
    spark = create_iceberg_spark()
    spark.sparkContext.setLogLevel('WARN')

    # Stream from Kafka
    kafka_df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', 'localhost:9092,kafka:9092') \
        .option('subscribe', 'ecommerce_order_events') \
        .load()

    parsed_df = kafka_df.selectExpr('CAST(value AS STRING) as json_str') \
        .select(from_json(col('json_str'), SCHEMA).alias('data')) \
        .select('data.*') \
        .withColumn('dt', to_date(col('event_timestamp')))

    # Write directly to Iceberg Table with ACID Guarantee
    query = parsed_df.writeStream \
        .format('iceberg') \
        .outputMode('append') \
        .trigger(processingTime='10 seconds') \
        .option('checkpointLocation', '/tmp/checkpoint-iceberg-orders') \
        .toTable('lakehouse.ecommerce.orders_realtime')

    query.awaitTermination()

if __name__ == '__main__':
    main()
