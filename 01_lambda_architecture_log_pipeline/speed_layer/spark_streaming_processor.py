import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

SCHEMA = StructType([
    StructField('event_id', StringType(), False),
    StructField('timestamp', StringType(), False),
    StructField('user_id', StringType(), False),
    StructField('action', StringType(), False),
    StructField('search_keyword', StringType(), True),
    StructField('document_id', StringType(), True),
    StructField('category', StringType(), True),
    StructField('duration_sec', DoubleType(), True),
    StructField('ip_address', StringType(), True),
    StructField('user_agent', StringType(), True)
])

def create_spark_session():
    return SparkSession.builder         .appName('ClinicalSearchLogSpeedLayer')         .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1')         .config('spark.sql.streaming.forceDeleteTempCheckpointLocation', 'true')         .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel('WARN')

    # Read from Kafka Topic
    kafka_df = spark.readStream         .format('kafka')         .option('kafka.bootstrap.servers', 'localhost:9092,kafka:9092')         .option('subscribe', 'clinical_search_logs')         .option('startingOffsets', 'latest')         .load()

    # Parse JSON
    parsed_df = kafka_df.selectExpr('CAST(value AS STRING) as json_str')         .select(from_json(col('json_str'), SCHEMA).alias('data'))         .select('data.*')         .withColumn('event_time', col('timestamp').cast('timestamp'))

    # 1. Real-time Trending Keywords (5-minute sliding window with 1-minute slide)
    trending_df = parsed_df         .filter(col('action') == 'search')         .withWatermark('event_time', '10 minutes')         .groupBy(window(col('event_time'), '5 minutes', '1 minute'), col('search_keyword'))         .agg(count('*').alias('search_count'))         .select(
            col('window.start').alias('window_start'),
            col('window.end').alias('window_end'),
            col('search_keyword'),
            col('search_count')
        )

    # 2. Anomaly Detection: Rapid Requests (>20 requests in 1 min by single user)
    anomaly_df = parsed_df         .withWatermark('event_time', '5 minutes')         .groupBy(window(col('event_time'), '1 minute', '30 seconds'), col('user_id'), col('ip_address'))         .agg(count('*').alias('request_count'))         .filter(col('request_count') > 20)         .select(
            col('window.start').alias('window_start'),
            col('window.end').alias('window_end'),
            col('user_id'),
            col('ip_address'),
            col('request_count'),
            current_timestamp().alias('detected_at')
        )

    # Output to Console / Realtime Sink
    query_trending = trending_df.writeStream         .outputMode('complete')         .format('console')         .option('truncate', 'false')         .start()

    query_trending.awaitTermination()

if __name__ == '__main__':
    main()
