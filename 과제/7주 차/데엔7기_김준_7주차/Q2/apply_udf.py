import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

def main():
    spark = SparkSession.builder \
        .appName("Q2_Apply_UDF") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    csv_path = "data.csv"
    if not os.path.exists(csv_path):
        csv_path = "/workspace/data.csv"

    # 1. data.csv 로드
    df = spark.read.csv(csv_path, header=True, inferSchema=True)

    # 2. @udf로 연령대 분류 UDF 정의 (20대: Young, 30대 이상: Senior)
    @F.udf(returnType=StringType())
    def categorize_age(age):
        if age is None:
            return "Unknown"
        elif age < 30:
            return "Young (20s)"
        else:
            return "Senior (30s+)"

    # 3. withColumn으로 UDF 적용하여 age_group 컬럼 추가
    udf_df = df.withColumn("age_group", categorize_age(F.col("age")))

    print("=== UDF 적용 결과 ===")
    udf_df.show()

    # 4. output/ 폴더에 결과 CSV 저장
    output_path = "/workspace/output" if os.path.exists("/workspace") else "output"
    udf_df.write.mode("overwrite").option("header", True).csv(output_path)
    print(f"=== 결과 파일 저장 완료: {output_path} ===")

    spark.stop()

if __name__ == "__main__":
    main()
