#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Problem 2: sensor_producer.py
센서 측정 데이터(sensor_readings.csv)를 읽어 Kafka의 sensor-readings 토픽으로 실시간 스트리밍 전송
"""

import os
import sys
import json
import time
import csv
import argparse
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092,localhost:9093,localhost:9094")
TOPIC_NAME = "sensor-readings"

def main():
    parser = argparse.ArgumentParser(description="Sensor Readings Kafka Producer")
    parser.add_argument("--csv-path", default="../data/sensor_readings.csv", help="Path to sensor_readings.csv")
    parser.add_argument("--delay", type=float, default=0.01, help="Delay between messages in seconds")
    args = parser.parse_args()

    csv_path = args.csv_path
    if not os.path.exists(csv_path):
        csv_path = os.path.join(os.path.dirname(__file__), "../data/sensor_readings.csv")
    if not os.path.exists(csv_path):
        csv_path = os.path.join(os.path.dirname(__file__), "data/sensor_readings.csv")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
        retries=3
    )

    count = 0
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            msg = {
                "equipment_id": int(row["equipment_id"]),
                "event_time": row["event_time"],
                "temperature": float(row["temperature"]),
                "vibration": float(row["vibration"]),
                "pressure": float(row["pressure"])
            }
            producer.send(TOPIC_NAME, key=msg["equipment_id"], value=msg)
            count += 1
            print(f"Sent [{count}]: {msg}", flush=True)
            if args.delay > 0:
                time.sleep(args.delay)

    producer.flush()
    print(f"\nDone. {count} messages sent to '{TOPIC_NAME}'.", flush=True)

if __name__ == "__main__":
    main()
