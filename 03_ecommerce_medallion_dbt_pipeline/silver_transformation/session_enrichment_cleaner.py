import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when, sum, coalesce, lit, to_timestamp, window
from pyspark.sql.window import Window

def transform_bronze_to_silver(bronze_path, silver_path):
    spark = SparkSession.builder \
        .appName('SilverSessionizationAndCleaning') \
        .getOrCreate()

    # Read Bronze
    bronze_df = spark.read.parquet(bronze_path)

    # 1. Schema Validation & Null Cleaning
    clean_df = bronze_df \
        .filter(col('user_id').isNotNull() & col('event_timestamp').isNotNull()) \
        .dropDuplicates(['event_id'])

    # 2. Sessionization (30-minute inactivity threshold)
    window_spec = Window.partitionBy('user_id').orderBy('event_timestamp')
    
    session_df = clean_df \
        .withColumn('prev_time', lag('event_timestamp').over(window_spec)) \
        .withColumn('time_diff_sec', (col('event_timestamp').cast('long') - col('prev_time').cast('long'))) \
        .withColumn('is_new_session', when((col('time_diff_sec') > 1800) | col('prev_time').isNull(), 1).otherwise(0)) \
        .withColumn('session_idx', sum('is_new_session').over(window_spec)) \
        .withColumn('session_id', concat_ws('_', col('user_id'), col('session_idx')))

    # Write to Silver Layer
    session_df.write \
        .mode('overwrite') \
        .partitionBy('event_date') \
        .parquet(silver_path)

    print('Silver layer transformation and sessionization finished.')
    spark.stop()

if __name__ == '__main__':
    transform_bronze_to_silver('/data/lake/bronze/clickstream/', '/data/lake/silver/clickstream/')
