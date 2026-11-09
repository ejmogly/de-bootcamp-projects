import os
import sys
import time
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ==============================================================================
# [과제 07: Spark on K8s 집계잡 → Redshift 적재]
# 작성자: 데엔 7기 김준
# ==============================================================================

BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "realestate-junekim")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
REDSHIFT_WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP", "de8-stock-wg")
REDSHIFT_DB = os.environ.get("REDSHIFT_DB", "dev")
REDSHIFT_ROLE_ARN = os.environ.get(
    "REDSHIFT_ROLE_ARN", "arn:aws:iam::268790346668:role/de8-redshift-role"
)

S3_SILVER_PREFIX = "stock/silver"
S3_OUTPUT_PREFIX = "stock/q8_output"

print(f"Starting Spark on K8s Stock Aggregation Job...")
print(f"Bucket: {BUCKET_NAME}, Region: {AWS_REGION}")

# Spark 세션 생성
spark = (
    SparkSession.builder.appName("q8-spark-stock")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. S3 Silver Parquet 데이터 로드
s3 = boto3.client("s3", region_name=AWS_REGION)
temp_silver_dir = "/tmp/stock_silver_input"
os.system(f"rm -rf {temp_silver_dir} && mkdir -p {temp_silver_dir}")

# S3에서 silver parquet 파일 다운로드
resp = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_SILVER_PREFIX)
for obj in resp.get("Contents", []):
    key = obj["Key"]
    if key.endswith(".parquet"):
        local_file = os.path.join(temp_silver_dir, os.path.basename(key))
        s3.download_file(BUCKET_NAME, key, local_file)

df_silver = spark.read.parquet(temp_silver_dir)

# 2. groupBy(ticker) 4대 지표 집계
df_agg = (
    df_silver.groupBy("ticker")
    .agg(
        F.round(F.avg("close"), 2).alias("avg_close"),
        F.round(F.max("high"), 2).alias("max_high"),
        F.round(F.min("low"), 2).alias("min_low"),
        F.sum("volume").alias("total_volume"),
    )
    .orderBy("ticker")
)

# 집계 결과 화면 출력 (문제지 32p 로그 형식 일치)
print("=== Q8 종목별 집계 결과 ===")
df_agg.show(truncate=False)

# 3. 집계 결과를 Parquet(snappy)로 S3 q8_output에 저장
temp_out_dir = "/tmp/q8_output"
os.system(f"rm -rf {temp_out_dir}")
df_agg.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(temp_out_dir)

# 기존 S3 q8_output 정리
resp_del = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=S3_OUTPUT_PREFIX)
if "Contents" in resp_del:
    del_keys = [{"Key": o["Key"]} for o in resp_del["Contents"]]
    s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": del_keys})

# S3로 업로드
saved_file_name = "part-00000.snappy.parquet"
for root, _, files in os.walk(temp_out_dir):
    for f in files:
        if f.endswith(".parquet"):
            local_p = os.path.join(root, f)
            s3_key = f"{S3_OUTPUT_PREFIX}/{saved_file_name}"
            s3.upload_file(local_p, BUCKET_NAME, s3_key)
            print(f"=== Q8 S3 저장 완료: s3://{BUCKET_NAME}/{s3_key} ===")

# 4. Redshift Data API를 통한 DDL 및 COPY 적재
print("=== Redshift 적재 시작 (Data API COPY) ===")
redshift = boto3.client("redshift-data", region_name=AWS_REGION)

# (1) DDL 실행
ddl_sql = """
CREATE TABLE IF NOT EXISTS stock_daily_stats (
    ticker       VARCHAR(10) PRIMARY KEY,
    avg_close    DOUBLE PRECISION,
    max_high     DOUBLE PRECISION,
    min_low      DOUBLE PRECISION,
    total_volume BIGINT
);
TRUNCATE TABLE stock_daily_stats;
"""
res_ddl = redshift.execute_statement(
    WorkgroupName=REDSHIFT_WORKGROUP,
    Database=REDSHIFT_DB,
    Sql=ddl_sql,
)
stmt_ddl_id = res_ddl["Id"]
while True:
    desc = redshift.describe_statement(Id=stmt_ddl_id)
    status = desc["Status"]
    if status in ["FINISHED", "FAILED"]:
        break
    time.sleep(1)

print(f"DDL 상태: {status}")
if status == "FAILED":
    print("DDL 에러:", desc.get("Error"))
    sys.exit(1)

# (2) COPY 실행
copy_sql = f"""
COPY stock_daily_stats
FROM 's3://{BUCKET_NAME}/{S3_OUTPUT_PREFIX}/'
IAM_ROLE '{REDSHIFT_ROLE_ARN}'
FORMAT AS PARQUET;
"""
res_copy = redshift.execute_statement(
    WorkgroupName=REDSHIFT_WORKGROUP,
    Database=REDSHIFT_DB,
    Sql=copy_sql,
)
stmt_copy_id = res_copy["Id"]
while True:
    desc = redshift.describe_statement(Id=stmt_copy_id)
    status = desc["Status"]
    if status in ["FINISHED", "FAILED"]:
        break
    time.sleep(1)

print(f"COPY 상태: {status}")
if status == "FAILED":
    print("COPY 에러:", desc.get("Error"))
    sys.exit(1)

# (3) 건수 확인
res_cnt = redshift.execute_statement(
    WorkgroupName=REDSHIFT_WORKGROUP,
    Database=REDSHIFT_DB,
    Sql="SELECT COUNT(*) FROM stock_daily_stats;",
)
stmt_cnt_id = res_cnt["Id"]
while True:
    desc = redshift.describe_statement(Id=stmt_cnt_id)
    if desc["Status"] in ["FINISHED", "FAILED"]:
        break
    time.sleep(1)

res_data = redshift.get_statement_result(Id=stmt_cnt_id)
row_count = res_data["Records"][0][0]["longValue"]
print(f"=== Redshift stock_daily_stats row count: {row_count} ===")

spark.stop()
print("Spark on K8s Stock Aggregation Job Completed Successfully!")
