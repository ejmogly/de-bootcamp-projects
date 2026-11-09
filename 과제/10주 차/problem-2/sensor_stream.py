#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Problem 2: sensor_stream.py
Spark Structured Streaming 기반 실시간 센서 스트림 수신,
파생 컬럼 생성, stream-static left_outer 조인, sensor-readings-enriched 토픽 재발행 및 Slack 이상 알림
"""

import os
import sys
import json
import time
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, abs, when
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "kafka-1:29092,kafka-2:29093,kafka-3:29094")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

def send_slack_alert(record):
    if not SLACK_WEBHOOK_URL:
        return
    eq_name = record.get("equipment_name") or "미등록설비"
    line_no = f"{record.get('line_no')}라인" if record.get("line_no") else ""
    p_type = record.get("process_type") or ""
    line_info = f"{line_no} {p_type}".strip() if (line_no or p_type) else "미지정라인"
    eq_id = record.get("equipment_id")
    temp = record.get("temperature", 0.0)
    dev = record.get("deviation", 0.0)
    e_time = record.get("event_time", "")

    text = f"[⚠️ 설비 이상 감지] {line_info} / {eq_name} / 온도 {temp}도 (기준 70도, 편차 {dev}) / {e_time}"
    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=3)
    except Exception as e:
        print(f"Slack send error: {e}")

def main():
    spark = SparkSession.builder \
        .appName("SensorStructuredStreaming") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 80)
    print("⚡ [Problem 2] Starting Spark Structured Streaming Pipeline")
    print(f"• Bootstrap Servers: {BOOTSTRAP_SERVERS}")
    print("=" * 80)

    # 1. Explicit Schemas
    sensor_schema = StructType([
        StructField("equipment_id", IntegerType(), True),
        StructField("event_time", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        StructField("pressure", DoubleType(), True)
    ])

    master_schema = StructType([
        StructField("equipment_id", IntegerType(), True),
        StructField("equipment_name", StringType(), True),
        StructField("line_no", IntegerType(), True),
        StructField("process_type", StringType(), True),
        StructField("install_date", IntegerType(), True)
    ])

    # 2. Static Master DataFrame from equipment-master topic (deduplicated to 48 rows)
    raw_master = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "equipment-master") \
        .option("startingOffsets", "earliest") \
        .load()

    master_df = raw_master.select(
        from_json(col("value").cast("string"), master_schema).alias("m")
    ).select(
        "m.equipment_id", "m.equipment_name", "m.line_no", "m.process_type"
    ).filter(col("equipment_id").isNotNull()).dropDuplicates(["equipment_id"])

    print(f"✅ Loaded Static Master Data: {master_df.count()} unique equipment records")

    # 3. Stream from sensor-readings topic
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "sensor-readings") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", 500) \
        .load()

    parsed_stream = raw_stream.select(
        from_json(col("value").cast("string"), sensor_schema).alias("s")
    ).select("s.*")

    # 4. Derived Columns: deviation & status
    transformed_stream = parsed_stream \
        .withColumn("deviation", abs(col("temperature") - 70.0)) \
        .withColumn("status", when(abs(col("temperature") - 70.0) > 20.0, "ALERT").otherwise("NORMAL"))

    # 5. Stream-Static Join: MUST be left_outer to preserve unmatched equipment 49 & 50
    joined_stream = transformed_stream.join(
        master_df,
        on="equipment_id",
        how="left_outer"
    )

    # 6. Kafka Enriched Topic Sink
    kafka_sink_df = joined_stream.select(
        col("equipment_id").cast("string").alias("key"),
        to_json(struct(
            col("equipment_id"),
            col("event_time"),
            col("temperature"),
            col("vibration"),
            col("pressure"),
            col("deviation"),
            col("status"),
            col("equipment_name"),
            col("line_no"),
            col("process_type")
        )).alias("value")
    )

    kafka_query = kafka_sink_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("topic", "sensor-readings-enriched") \
        .option("checkpointLocation", "/tmp/spark_checkpoint_enriched") \
        .outputMode("append") \
        .start()

    # 7. ForeachBatch: Console print & Slack notifications
    def process_batch(batch_df, batch_id):
        alert_df = batch_df.filter(col("status") == "ALERT")
        alert_count = alert_df.count()

        print(f"\n=== Batch: {batch_id} (ALERT 필터 콘솔 출력) ===", flush=True)
        alert_df.select(
            "equipment_id", "event_time", "temperature", "vibration", "pressure",
            "deviation", "status", "equipment_name", "line_no", "process_type"
        ).show(truncate=False)

        # Send sample alerts to Slack
        if alert_count > 0:
            sample_records = alert_df.limit(2).collect()
            for r in sample_records:
                send_slack_alert(r.asDict())
                time.sleep(0.1)

    console_query = joined_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
