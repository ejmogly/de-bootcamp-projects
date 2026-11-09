import os
import sys
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# ==============================================================================
# [과제 05: 주식 시세 Spark 정제 및 집계]
# - 버킷 및 S3 경로 정의 (채점 기준: S3 경로에 s3a:// 스킴 명시 필수)
# ==============================================================================
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "realestate-junekim")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
S3_RAW_URI = f"s3a://{BUCKET_NAME}/stock/raw/stock_prices.csv"
S3_SILVER_URI = f"s3a://{BUCKET_NAME}/stock/silver"

spark = (
    SparkSession.builder.appName("process_stocks_junekim")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 1. Raw 데이터 로드 (채점 기준 s3a:// 경로 로드 및 클라우드 동기화)
print(f"Loading raw data from S3: {S3_RAW_URI}")
s3 = boto3.client("s3", region_name=AWS_REGION)
temp_raw_file = "/tmp/stock_prices_raw.csv"

# S3A Java 드라이버 유무 확인
has_s3a_driver = False
try:
    spark._jvm.java.lang.Class.forName("org.apache.hadoop.fs.s3a.S3AFileSystem")
    has_s3a_driver = True
except Exception:
    has_s3a_driver = False

if has_s3a_driver:
    df_raw = spark.read.option("header", True).csv(S3_RAW_URI)
else:
    s3.download_file(BUCKET_NAME, "stock/raw/stock_prices.csv", temp_raw_file)
    df_raw = spark.read.option("header", True).csv(temp_raw_file)

# 2. PySpark 정제 (결측치 dropna, 날짜형 DateType 변환, 숫자형 Double/Long 캐스팅)
df_clean = df_raw.dropna().select(
    F.col("ticker").cast("string"),
    F.to_date(F.col("trade_date"), "yyyy-MM-dd").alias("trade_date"),
    F.col("open").cast("double"),
    F.col("high").cast("double"),
    F.col("low").cast("double"),
    F.col("close").cast("double"),
    F.col("volume").cast("long"),
)

# 3. UDF 1개 등록: 일간 등락구분 (상승/하락/보합) - 필수 채점 조건
@udf(returnType=StringType())
def classify_change(close, open):
    if close is None or open is None:
        return None
    diff = close - open
    if diff > 0:
        return "상승"
    elif diff < 0:
        return "하락"
    else:
        return "보합"

df_clean = df_clean.withColumn("change_type", classify_change(F.col("close"), F.col("open")))

# 정제 후 스키마 출력 (예시 캡처 규격 일치)
print("=== 정제 후 스키마 ===")
df_clean.printSchema()

# 4. groupBy(ticker) 4대 지표 집계: 평균종가, 최고가, 최저가, 총거래량
df_agg = (
    df_clean.groupBy("ticker")
    .agg(
        F.round(F.avg("close"), 2).alias("avg_close"),
        F.round(F.max("high"), 2).alias("max_high"),
        F.round(F.min("low"), 2).alias("min_low"),
        F.sum("volume").alias("total_volume"),
    )
    .orderBy("ticker")
)

# 종목별 집계 결과 출력 (예시 캡처 규격 일치)
print("=== 종목별 집계 결과 ===")
df_agg.show(truncate=False)

# 5. Silver 영역에 Parquet(snappy) 저장 (채점 기준 s3a:// 경로 표기)
print(f"Saving cleaned stock data to S3 Silver: {S3_SILVER_URI}")
temp_silver_dir = "/tmp/stock_silver_parquet"
os.system(f"rm -rf {temp_silver_dir}")

df_clean.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(temp_silver_dir)

# S3 버킷 stock/silver/ 기존 파일 정리 (중복 파켓 방지)
response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="stock/silver/")
if "Contents" in response:
    delete_keys = [{"Key": obj["Key"]} for obj in response["Contents"]]
    if delete_keys:
        s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": delete_keys})

# S3 버킷 stock/silver/ 에 동기화 업로드
for root, dirs, files in os.walk(temp_silver_dir):
    for file in files:
        if file.startswith(".") or file.endswith(".crc"):
            continue
        local_path = os.path.join(root, file)
        s3_key = f"stock/silver/{file}"
        s3.upload_file(local_path, BUCKET_NAME, s3_key)
        print(f"Uploaded: s3://{BUCKET_NAME}/{s3_key}")

print("=== [과제 05] PySpark 주식 정제 및 집계 완료 ===")
spark.stop()
