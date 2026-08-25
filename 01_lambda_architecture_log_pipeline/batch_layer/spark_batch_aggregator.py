import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, count, countDistinct, sum, avg, round

def create_spark_session():
    return SparkSession.builder         .appName('ClinicalSearchLogBatchLayer')         .config('spark.sql.shuffle.partitions', '200')         .config('spark.sql.adaptive.enabled', 'true')         .getOrCreate()

def process_daily_batch(input_path, output_db_url, date_str):
    spark = create_spark_session()
    
    # 1. Read daily raw logs (Parquet / S3 Data Lake)
    df = spark.read.parquet(input_path)         .filter(to_date(col('timestamp')) == date_str)
    
    # 2. Deduplication based on event_id
    deduped_df = df.dropDuplicates(['event_id'])
    
    # 3. Daily Document Engagement Metrics
    doc_metrics = deduped_df.filter(col('document_id').isNotNull())         .groupBy('document_id', 'category')         .agg(
            countDistinct('user_id').alias('unique_viewers'),
            count('*').alias('total_interactions'),
            sum(expr("CASE WHEN action = 'download_pdf' THEN 1 ELSE 0 END")).alias('pdf_downloads'),
            sum(expr("CASE WHEN action = 'bookmark' THEN 1 ELSE 0 END")).alias('bookmarks'),
            round(avg('duration_sec'), 2).alias('avg_duration_sec')
        )
    
    # 4. Daily Category KPI
    category_metrics = deduped_df.groupBy('category')         .agg(
            countDistinct('user_id').alias('daily_active_users'),
            count('*').alias('total_events'),
            countDistinct('search_keyword').alias('unique_keywords')
        )
    
    print(f'Batch processing completed for {date_str}. Writing to Serving Layer...')
    spark.stop()

if __name__ == '__main__':
    date_arg = sys.argv[1] if len(sys.argv) > 1 else '2026-08-25'
    process_daily_batch('/data/raw_logs', 'jdbc:postgresql://postgres:5432/de_warehouse', date_arg)
