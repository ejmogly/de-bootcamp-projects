#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Problem 5: fault_producer.py
브로커 장애(kafka-2 다운) 상황에서 sensor-readings 토픽으로 600건 전송
- acks=all, retries=0
- 파티션별 end offset 전/후 비교
- 파티션별 성공/실패/예외 클래스 집계 및 출력
"""

import os
import sys
import time
import json
from collections import defaultdict
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094")
TOPIC_NAME = "sensor-readings"
TOTAL_RECORDS = 600
NUM_PARTITIONS = 6

def get_end_offsets(consumer, partitions):
    return {tp.partition: consumer.end_offsets([tp])[tp] for tp in partitions}

def main():
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S %Z')}] Starting Fault Injection Producer Test (600 records)")
    print(f"• Target Topic: {TOPIC_NAME} (Partitions: {NUM_PARTITIONS})")
    print(f"• Bootstrap Servers: {BOOTSTRAP_SERVERS}")
    print(f"• Producer Configs: acks=all, retries=0, request_timeout_ms=3000\n")

    # 1. Query Initial End Offsets
    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        enable_auto_commit=False,
        request_timeout_ms=5000
    )
    partitions = [TopicPartition(TOPIC_NAME, p) for p in range(NUM_PARTITIONS)]
    initial_offsets = get_end_offsets(consumer, partitions)

    # 2. Producer with acks=all, retries=0
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=0,
        request_timeout_ms=3000
    )

    success_counts = defaultdict(int)
    failure_counts = defaultdict(int)
    exception_types = defaultdict(set)

    print("Sending 600 messages evenly across partitions...")
    for i in range(TOTAL_RECORDS):
        partition_target = i % NUM_PARTITIONS
        payload = {
            "equipment_id": (i % 48) + 1,
            "event_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": 75.0,
            "vibration": 50.0,
            "pressure": 3.0
        }
        try:
            future = producer.send(TOPIC_NAME, key=payload["equipment_id"], value=payload, partition=partition_target)
            record_metadata = future.get(timeout=4)
            p = record_metadata.partition
            success_counts[p] += 1
        except Exception as e:
            exc_name = type(e).__name__
            failure_counts[partition_target] += 1
            exception_types[partition_target].add(exc_name)

    producer.close()

    # 3. Query Final End Offsets
    final_offsets = get_end_offsets(consumer, partitions)
    consumer.close()

    # 4. Print Verification Table
    print("\n" + "=" * 80)
    print("📊 [장애 중 600건 전송 결과표 (Fault Producer Result)]")
    print("=" * 80)
    print(f"{'Partition':<10} | {'Sent':<6} | {'Success':<8} | {'Failed':<8} | {'Offset Delta':<12} | {'Exception Class'}")
    print("-" * 80)

    total_sent = 0
    total_success = 0
    total_failed = 0

    for p in range(NUM_PARTITIONS):
        succ = success_counts[p]
        fail = failure_counts[p]
        sent = succ + fail
        delta = final_offsets[p] - initial_offsets[p]
        exc_str = ", ".join(exception_types[p]) if exception_types[p] else "-"

        total_sent += sent
        total_success += succ
        total_failed += fail

        print(f"Partition {p:<2} | {sent:<6} | {succ:<8} | {fail:<8} | {delta:<12} | {exc_str}")

    print("-" * 80)
    print(f"{'Total':<10} | {total_sent:<6} | {total_success:<8} | {total_failed:<8} | {total_success:<12} | -")
    print("=" * 80)
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S %Z')}] Test Completed.\n")

if __name__ == "__main__":
    main()
