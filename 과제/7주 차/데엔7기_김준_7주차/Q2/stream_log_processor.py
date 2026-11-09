import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    host = sys.argv[1] if len(sys.argv) > 1 else os.getenv("SOCKET_HOST", "localhost")
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 9999

    spark = SparkSession.builder \
        .appName("Q2_Stream_Log_Processor") \
        .master("local[*]") \
        .getOrCreate()

    # 1. Socket 스트리밍 소스
    lines = spark.readStream \
        .format("socket") \
        .option("host", host) \
        .option("port", port) \
        .load()

    # 2. ERROR 필터 및 count 집계
    error_lines = lines.filter(F.col("value").contains("ERROR"))
    error_counts = error_lines.groupBy().count()

    # 3. 콘솔 출력 싱크
    query = error_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="2 seconds") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
