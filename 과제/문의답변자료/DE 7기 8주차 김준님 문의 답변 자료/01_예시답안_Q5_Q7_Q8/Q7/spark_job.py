"""과제 07 — Spark on K8s 집계잡 → Redshift 적재

과제 05 의 정제·집계 로직(process_stocks.py)을 그대로 재사용하고,
집계 결과를 S3 에 parquet 으로 저장한 뒤 Redshift Data API 로 COPY 적재한다.

실행 (K8s Job 컨테이너 안):
  spark-submit --master k8s://https://kubernetes.default.svc:443 --deploy-mode client ... \
    local:///opt/spark/work-dir/spark_job.py \
    --input  s3a://<본인버킷>/stock/raw/stock_prices.csv \
    --output s3a://<본인버킷>/stock/daily_stats/

환경 변수 (Redshift 적재 시 필요, 없으면 적재 단계를 건너뛴다):
  REDSHIFT_WORKGROUP, REDSHIFT_DATABASE, REDSHIFT_IAM_ROLE_ARN, AWS_REGION

자격 증명:
  코드·이미지·YAML 에 키를 두지 않는다. S3A 와 boto3 모두 기본 자격 증명 체인으로
  EC2 인스턴스 IAM Role 을 사용한다(임시 자격 증명은 SDK 가 만료 전에 자동 갱신).
  S3A 는 출력 커밋 과정에서 임시 경로를 정리하므로 Role 에 s3:DeleteObject 권한이 필요하다.
"""
import argparse
import os

import boto3
from pyspark.sql import SparkSession

from process_stocks import RAW_SCHEMA, aggregate, clean
from redshift_loader import RedshiftDataLoader, replace_table_sqls

STATS_TABLE = "stock_daily_stats"
# parquet COPY 는 열 순서로 매핑되므로, 저장 컬럼 순서와 DDL 순서를 같이 관리한다.
STATS_COLUMNS = [
    ("ticker", "VARCHAR(10)"),
    ("avg_close", "DOUBLE PRECISION"),
    ("max_high", "DOUBLE PRECISION"),
    ("min_low", "DOUBLE PRECISION"),
    ("total_volume", "BIGINT"),
]


def to_copy_uri(s3a_uri: str) -> str:
    """Spark 경로(s3a://)를 Redshift COPY 경로(s3://)로 바꾼다."""
    return "s3://" + s3a_uri.split("://", 1)[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="과제 07 Spark on K8s → Redshift")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True, help="집계 결과 parquet 경로")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("q7_spark_on_k8s").getOrCreate()
    try:
        print(f"=== Spark master: {spark.sparkContext.master} ===")
        raw = spark.read.option("header", "true").schema(RAW_SCHEMA).csv(args.input)
        stats = aggregate(clean(raw)).select(*[name for name, _ in STATS_COLUMNS])

        print("=== 종목별 집계 결과 ===")
        stats.show(truncate=False)

        # 집계 결과는 종목 수(5행)만큼이라 파일 1개로 저장한다.
        stats.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(args.output)
        print(f"=== S3 저장 완료: {args.output} ===")
    finally:
        spark.stop()

    workgroup = os.environ.get("REDSHIFT_WORKGROUP")
    if not workgroup:
        print("=== REDSHIFT_WORKGROUP 미설정: Redshift 적재 단계 건너뜀 ===")
        return

    loader = RedshiftDataLoader(
        client=boto3.client("redshift-data", region_name=os.environ.get("AWS_REGION", "ap-northeast-2")),
        workgroup=workgroup,
        database=os.environ["REDSHIFT_DATABASE"],
    )
    ddl_columns = ", ".join(f"{name} {dtype}" for name, dtype in STATS_COLUMNS)
    loader.run_transaction(replace_table_sqls(
        STATS_TABLE, ddl_columns, to_copy_uri(args.output), os.environ["REDSHIFT_IAM_ROLE_ARN"]
    ))
    print("=== Redshift COPY 적재 완료 (트랜잭션) ===")
    print(f"=== Redshift {STATS_TABLE} row count: {loader.query_scalar(f'SELECT COUNT(*) FROM {STATS_TABLE}')} ===")


if __name__ == "__main__":
    main()
