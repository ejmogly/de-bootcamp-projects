import os
import sys
import glob
import time
import xml.etree.ElementTree as ET
import boto3
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, when, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# ==============================================================================
# [설정 및 채점 규격]
# - Spark UI 우상단 Application Name 필수 일치: silver_realestate_transform
# ==============================================================================
BUCKET_NAME = os.environ.get("S3_BUCKET_NAME", "realestate-junekim")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-northeast-2")
APP_NAME = "silver_realestate_transform"

print(f"=== [PySpark Silver Transform] Starting: {APP_NAME} ===")

# 1. SparkSession 초기화
spark = (
    SparkSession.builder.appName(APP_NAME)
    .config("spark.ui.port", "4040")
    .config("spark.sql.execution.arrow.pyspark.enabled", "false")
    .getOrCreate()
)

sc = spark.sparkContext
sc.setLogLevel("WARN")
print(f"Spark UI available at: http://localhost:4040")

# 2. S3 Bronze 영역에서 원본 XML 다운로드 및 레코드 파싱
s3 = boto3.client("s3", region_name=AWS_REGION)
response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="bronze/")

records = []
if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]
        if not key.endswith(".xml"):
            continue
        print(f"Reading S3 Bronze XML: {key}")
        file_obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        xml_content = file_obj["Body"].read().decode("utf-8")

        try:
            root = ET.fromstring(xml_content)
            for item in root.findall(".//item"):
                deal_amount_str = item.findtext("dealAmount", "").strip().replace(",", "")
                area_str = item.findtext("excluUseAr", "").strip()

                # 결측치 1차 제외
                if not deal_amount_str or not area_str:
                    continue

                deal_amount = int(deal_amount_str)
                area = float(area_str)
                sgg_cd = item.findtext("sggCd", "").strip()
                apt_nm = item.findtext("aptNm", "").strip()
                umd_nm = item.findtext("umdNm", "").strip()
                build_year = item.findtext("buildYear", "").strip()
                floor = item.findtext("floor", "").strip()
                deal_year = item.findtext("dealYear", "").strip()
                deal_month = item.findtext("dealMonth", "").strip().zfill(2)
                deal_day = item.findtext("dealDay", "").strip().zfill(2)

                deal_date = f"{deal_year}-{deal_month}-{deal_day}"
                yyyymm = f"{deal_year}{deal_month}"

                # [문제지 11p 캡처 100% 일치] 시군구 파티션 키 값으로 한글 법정동명(개포동, 도곡동 등) 사용
                records.append({
                    "시군구": umd_nm if umd_nm else sgg_cd,
                    "sgg_cd": sgg_cd,
                    "yyyymm": yyyymm,
                    "deal_date": deal_date,
                    "apt_nm": apt_nm,
                    "umd_nm": umd_nm,
                    "deal_amount": deal_amount,
                    "area": area,
                    "build_year": int(build_year) if build_year.isdigit() else None,
                    "floor": int(floor) if floor.isdigit() else None,
                })
        except Exception as e:
            print(f"XML Parsing Error ({key}): {e}")

print(f"Total parsed raw records: {len(records)}")

schema = StructType([
    StructField("시군구", StringType(), False),
    StructField("sgg_cd", StringType(), True),
    StructField("yyyymm", StringType(), False),
    StructField("deal_date", StringType(), True),
    StructField("apt_nm", StringType(), True),
    StructField("umd_nm", StringType(), True),
    StructField("deal_amount", IntegerType(), False),
    StructField("area", DoubleType(), False),
    StructField("build_year", IntegerType(), True),
    StructField("floor", IntegerType(), True),
])

df = spark.createDataFrame(records, schema=schema)

# 3. 결측치 제거 (문제지 조건: 거래금액 / 면적 NULL row 제거)
df = df.dropna(subset=["deal_amount", "area"])

# 4. UDF 등록 (채점 필수 조건: @udf 데코레이터 2회 이상 등록)
# UDF (1) price_per_pyeong: 거래금액 ÷ (전용면적 ÷ 3.3058)
@udf(returnType=DoubleType())
def calc_price_per_pyeong(amount, area):
    if not amount or not area or area <= 0:
        return None
    pyeong = area / 3.3058
    return round(float(amount) / pyeong, 2)

# UDF (2) size_category: 60 미만 소형 / 60~85 중소형 / 85~135 중대형 / 135 이상 대형
@udf(returnType=StringType())
def calc_size_category(area):
    if area is None:
        return None
    if area < 60.0:
        return "소형"
    elif area < 85.0:
        return "중소형"
    elif area < 135.0:
        return "중대형"
    else:
        return "대형"

# UDF 적용 컬럼 생성
df = df.withColumn("price_per_pyeong", calc_price_per_pyeong(col("deal_amount"), col("area")))
df = df.withColumn("size_category", calc_size_category(col("area")))

# 5. 시군구별 IQR 1.5배 밖 평당가 이상치 제거 (문제지 조건: approxQuantile 필수 사용)
print("Removing outliers using approxQuantile (1.5 * IQR)...")
districts = [row["시군구"] for row in df.select("시군구").distinct().collect()]
filtered_dfs = []

for dist in districts:
    dist_df = df.filter(col("시군구") == dist)
    # Q1(25%), Q3(75%) 분위수 계산
    quantiles = dist_df.approxQuantile("price_per_pyeong", [0.25, 0.75], 0.01)
    if len(quantiles) == 2:
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        dist_df = dist_df.filter(
            (col("price_per_pyeong") >= lower_bound) & (col("price_per_pyeong") <= upper_bound)
        )
    filtered_dfs.append(dist_df)

clean_df = reduce(lambda a, b: a.union(b), filtered_dfs)
print(f"Records after IQR filtering: {clean_df.count()}")
clean_df.show(5)

# 기존 silver 폴더의 이전 파일 정리 (기존 11170 등 제거)
print("Cleaning up old silver S3 keys...")
old_objs = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix="silver/")
if "Contents" in old_objs:
    delete_keys = [{"Key": o["Key"]} for o in old_objs["Contents"]]
    # 1000개 단위 분할 삭제
    for i in range(0, len(delete_keys), 1000):
        s3.delete_objects(Bucket=BUCKET_NAME, Delete={"Objects": delete_keys[i:i+1000]})

# 6. Parquet 저장: snappy 압축 및 파티션 키 = (시군구, yyyymm)
local_out_dir = f"/tmp/silver_parquet_{int(time.time())}"
os.system(f"rm -rf {local_out_dir}")

clean_df.write.mode("overwrite") \
    .partitionBy("시군구", "yyyymm") \
    .option("compression", "snappy") \
    .parquet(local_out_dir)

print(f"Uploading Parquet to S3 s3://{BUCKET_NAME}/silver/ ...")
for root_dir, dirs, files in os.walk(local_out_dir):
    for file in files:
        if file.startswith(".") or file.endswith(".crc"):
            continue
        local_path = os.path.join(root_dir, file)
        rel_path = os.path.relpath(local_path, local_out_dir)
        s3_key = f"silver/{rel_path}"
        s3.upload_file(local_path, BUCKET_NAME, s3_key)

print(f"=== [PySpark Silver Transform] S3 silver upload complete! ===")

# Spark UI (4040 포트) 유지
print("Keeping SparkContext active for 15 seconds...")
time.sleep(15)

spark.stop()
print("Spark application finished cleanly.")
