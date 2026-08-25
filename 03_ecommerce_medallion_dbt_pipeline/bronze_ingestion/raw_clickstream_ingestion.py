import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, input_file_name

def ingest_raw_to_bronze(source_dir, bronze_target_path):
    spark = SparkSession.builder \
        .appName('BronzeClickstreamIngestion') \
        .getOrCreate()

    # Read raw json/csv files (Append-Only)
    raw_df = spark.read.json(source_dir)

    # Add Ingestion Metadata
    bronze_df = raw_df \
        .withColumn('_ingested_at', current_timestamp()) \
        .withColumn('_source_file', input_file_name())

    # Write to Bronze Layer (Delta / Parquet)
    bronze_df.write \
        .mode('append') \
        .partitionBy('event_date') \
        .parquet(bronze_target_path)

    print(f'Bronze ingestion completed: {bronze_df.count()} records ingested.')
    spark.stop()

if __name__ == '__main__':
    ingest_raw_to_bronze('/data/raw_clickstream/', '/data/lake/bronze/clickstream/')
