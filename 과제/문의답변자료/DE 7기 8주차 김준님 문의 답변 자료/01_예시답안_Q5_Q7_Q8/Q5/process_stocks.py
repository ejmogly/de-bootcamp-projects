"""과제 05 — 주식 시세 정제·집계 (spark-submit 실행)

실행:
  spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    process_stocks.py \
    --input  s3a://<본인버킷>/stock/raw/stock_prices.csv \
    --output s3a://<본인버킷>/stock/silver/

자격 증명:
  코드에서 자격 증명 방식을 고정하지 않는다. S3A 기본 체인이 환경 변수 → EC2 인스턴스 IAM Role 순으로 찾는다.
  로컬에서 ~/.aws 프로필을 쓰려면 실행 시 아래 옵션을 추가한다.
    --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.profile.ProfileCredentialsProvider

과제 07(spark_job.py)이 이 모듈의 RAW_SCHEMA · clean · aggregate 를 그대로 재사용한다.
"""
import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# raw CSV 는 문자열로 읽은 뒤 명시적으로 캐스팅한다 (inferSchema 비용·추론 오류 방지).
RAW_SCHEMA = T.StructType([
    T.StructField(name, T.StringType())
    for name in ["ticker", "trade_date", "open", "high", "low", "close", "volume"]
])


@F.udf(returnType=T.StringType())
def change_type(open_price, close_price):
    """일간 등락 구분: 상승 / 하락 / 보합."""
    if open_price is None or close_price is None:
        return None
    if close_price > open_price:
        return "상승"
    if close_price < open_price:
        return "하락"
    return "보합"


def clean(raw: DataFrame) -> DataFrame:
    """타입 변환 후 결측(원래 결측 + 변환 실패)을 제거하고 등락 구분 컬럼을 추가한다."""
    typed = raw.select(
        F.trim("ticker").alias("ticker"),
        F.to_date("trade_date", "yyyy-MM-dd").alias("trade_date"),
        F.col("open").cast("double").alias("open"),
        F.col("high").cast("double").alias("high"),
        F.col("low").cast("double").alias("low"),
        F.col("close").cast("double").alias("close"),
        F.col("volume").cast("long").alias("volume"),
    )
    return typed.dropna().withColumn("change_type", change_type("open", "close"))


def aggregate(silver: DataFrame) -> DataFrame:
    """종목별 평균종가 · 최고가 · 최저가 · 총거래량."""
    return (
        silver.groupBy("ticker")
        .agg(
            F.avg("close").alias("avg_close"),
            F.max("high").alias("max_high"),
            F.min("low").alias("min_low"),
            F.sum("volume").alias("total_volume"),
        )
        .orderBy("ticker")
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="과제 05 주식 시세 정제·집계")
    parser.add_argument("--input", required=True, help="raw CSV 경로 (예: s3a://버킷/stock/raw/stock_prices.csv)")
    parser.add_argument("--output", required=True, help="silver parquet 경로 (예: s3a://버킷/stock/silver/)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("q5_process_stocks").getOrCreate()
    try:
        raw = spark.read.option("header", "true").schema(RAW_SCHEMA).csv(args.input)
        silver = clean(raw).cache()

        raw_rows, silver_rows = raw.count(), silver.count()
        print(f"=== 정제: raw {raw_rows}행 → silver {silver_rows}행 (제거 {raw_rows - silver_rows}행) ===")
        silver.printSchema()

        print("=== 종목별 집계 결과 ===")
        aggregate(silver).show(truncate=False)

        # 과제 06 에서 Redshift COPY 로 그대로 적재하므로 파티션 디렉터리 없이 평평하게 저장한다.
        silver.write.mode("overwrite").option("compression", "snappy").parquet(args.output)
        print(f"=== silver 저장 완료: {args.output} ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
