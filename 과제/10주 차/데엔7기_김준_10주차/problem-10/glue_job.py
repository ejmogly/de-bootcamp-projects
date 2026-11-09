import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, round as spark_round, sum as spark_sum
from pyspark.sql.types import IntegerType, DoubleType

# 1. Initialize Glue and Spark Contexts
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# S3 Base Paths
S3_BUCKET = "de7-junekim-week10"
ORDERS_PATH = f"s3://{S3_BUCKET}/topics/order-events/"
PRODUCTS_PATH = f"s3://{S3_BUCKET}/data/product_master.csv"
OUTPUT_PATH = f"s3://{S3_BUCKET}/curated/category_revenue/"

print("=" * 60)
print(f"Starting Glue ETL Job: {args['JOB_NAME']}")
print(f"Reading order events from: {ORDERS_PATH}")
print(f"Reading product master from: {PRODUCTS_PATH}")
print("=" * 60)

# 2. Read Order Events (All 8 Monthly Partitions, 5,049 rows)
# Spark automatically recurses subdirectories (year=YYYY/month=MM)
orders_df = spark.read.json(ORDERS_PATH)
orders_df = orders_df.withColumn("quantity", col("quantity").cast(IntegerType()))

order_count = orders_df.count()
print(f"Loaded {order_count} order events from all partitions.")

# 3. Read Product Master CSV (90 rows)
products_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(PRODUCTS_PATH)

products_df = products_df.withColumn("price", col("price").cast(DoubleType()))
product_count = products_df.count()
print(f"Loaded {product_count} product master records.")

# 4. Distributed Join & Category Revenue Aggregation
# Inner join drops 55 unmatched product orders, exactly matching Spectrum (4,994 joined)
joined_df = orders_df.join(products_df, on="product_id", how="inner")

category_rev_df = joined_df \
    .groupBy("category") \
    .agg(spark_round(spark_sum(col("quantity") * col("price")), 2).alias("revenue")) \
    .orderBy(col("revenue").desc())

print("Top Categories by Revenue:")
category_rev_df.show(10, truncate=False)

# 5. Write Results as Parquet to Curated S3 Path
print(f"Saving aggregated results as Parquet to: {OUTPUT_PATH}")
category_rev_df \
    .repartition(1) \
    .write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("ETL Job successfully completed.")
job.commit()
