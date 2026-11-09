#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Problem 4: tuning_test.py
Producer 튜닝 전/후 처리량 및 소요시간 성능 벤치마크 테스트
- 튜닝 전: batch.size=16384, linger.ms=0, compression.type=none, acks=all
- 튜닝 후: batch.size=131072, linger.ms=50, compression.type=lz4, acks=1
"""

import os
import sys
import time
import json
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094")
TOPIC_NAME = "sensor-readings"
TOTAL_RECORDS = 20000

SAMPLE_PAYLOAD = {
    "equipment_id": 1,
    "event_time": "2025-01-01 00:00:00",
    "temperature": 72.5,
    "vibration": 45.2,
    "pressure": 3.1
}

def run_benchmark(config_name, producer_configs, num_records=TOTAL_RECORDS):
    print(f"\n=======================================================")
    print(f"🚀 Running Benchmark: [{config_name}] ({num_records} records)")
    print(f"⚙️ Configs: {producer_configs}")
    print(f"=======================================================")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        **producer_configs
    )

    start_time = time.time()
    for i in range(num_records):
        producer.send(TOPIC_NAME, key=i % 48 + 1, value=SAMPLE_PAYLOAD)
    
    producer.flush()
    elapsed = time.time() - start_time
    throughput = num_records / elapsed
    print(f"✅ [{config_name}] Finished in {elapsed:.3f}s -> {throughput:.1f} msg/s")
    producer.close()
    return elapsed, throughput

def main():
    print("Kafka Producer Performance Tuning Benchmark")
    print(f"Target Topic: {TOPIC_NAME} (6 Partitions)")
    print(f"Records per test: {TOTAL_RECORDS}")

    # 1. 튜닝 전 (Default / Low Throughput)
    before_configs = {
        "batch_size": 16384,
        "linger_ms": 0,
        "compression_type": None,
        "acks": "all",
        "retries": 3
    }

    # 2. 튜닝 후 (High Throughput / Optimized)
    after_configs = {
        "batch_size": 131072,
        "linger_ms": 50,
        "compression_type": "lz4",
        "acks": 1,
        "retries": 3
    }

    print("\n[Step 1] Starting Benchmark: 튜닝 전 (Before)")
    t_before, tp_before = run_benchmark("Before Tuning", before_configs)

    print("\nWaiting 10s between tests for Grafana metric distinction...")
    time.sleep(10)

    print("\n[Step 2] Starting Benchmark: 튜닝 후 (After)")
    t_after, tp_after = run_benchmark("After Tuning", after_configs)

    improvement_pct = ((tp_after - tp_before) / tp_before) * 100

    print("\n" + "=" * 60)
    print("📊 [Final Performance Benchmark Summary]")
    print("=" * 60)
    print(f"• 튜닝 전: {t_before:.2f}초 | {tp_before:.1f} msg/s")
    print(f"• 튜닝 후: {t_after:.2f}초 | {tp_after:.1f} msg/s")
    print(f"• 처리량 개선율: +{improvement_pct:.1f}%")
    print("=" * 60)

if __name__ == "__main__":
    main()
