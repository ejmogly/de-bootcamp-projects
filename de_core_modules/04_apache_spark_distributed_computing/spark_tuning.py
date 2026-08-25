from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast

spark = SparkSession.builder \
    .appName('SparkPerformanceTuning') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
    .getOrCreate()

# Broadcast Join for Small Lookup Table (Avoids Shuffling)
large_df = spark.read.parquet('/data/large_events')
small_lookup = spark.read.parquet('/data/dim_categories')

joined_df = large_df.join(broadcast(small_lookup), 'category_id')
print('Broadcast join completed with zero shuffle overhead.')
