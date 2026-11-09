import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    spark = SparkSession.builder \
        .appName("Q2_Analyze_CSV") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    csv_path = "data.csv"
    if not os.path.exists(csv_path):
        csv_path = "/workspace/data.csv"

    # 1. read.csv로 data.csv 로드 (헤더 포함, 타입 자동 추론)
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    print("=== 1. 원본 데이터 ===")
    df.show()

    # 2. agg로 age 컬럼의 평균, 최댓값, 개수 계산
    agg_df = df.agg(
        F.avg("age").alias("avg_age"),
        F.max("age").alias("max_age"),
        F.count("name").alias("total_count")
    )

    print("=== 2. 집계 결과 (평균·최댓값·개수) ===")
    agg_df.show()

    spark.stop()

if __name__ == "__main__":
    main()
