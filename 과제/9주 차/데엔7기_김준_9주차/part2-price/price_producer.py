#!/usr/bin/env python3
"""
[Part 2 / Q6] 식료품 가격 배치 Producer
- 대상 데이터: data/india_grocery_prices.csv (인도 10대 도시 x 10개 품목 x 2016년 11/12월, 총 200건)
- 대상 토픽: product-prices
- 메시지 스키마: item_name, market, year, month, price, currency, unit
- 파티션 분산: P0 (68건), P1 (68건), P2 (64건) -> 총 200건
- 자원 관리: try...finally 구문으로 flush() 및 close() 보장
"""

import csv
import json
import os
import sys
import time
import warnings
from kafka import KafkaProducer

warnings.filterwarnings("ignore")

BOOTSTRAP_SERVERS = [
    "localhost:19092",
    "localhost:19093",
    "localhost:19094",
    "localhost:9092",
]
TOPIC_NAME = "product-prices"

def find_csv_file():
    candidates = [
        os.path.join(os.path.dirname(__file__), "..", "data", "india_grocery_prices.csv"),
        os.path.join(os.path.dirname(__file__), "data", "india_grocery_prices.csv"),
        os.path.join(os.path.dirname(__file__), "india_grocery_prices.csv"),
        os.path.join("..", "data", "india_grocery_prices.csv"),
        os.path.join("data", "india_grocery_prices.csv"),
        "india_grocery_prices.csv",
    ]
    for c in candidates:
        if os.path.exists(c):
            return os.path.abspath(c)
    raise FileNotFoundError(f"india_grocery_prices.csv 파일을 찾을 수 없습니다: {candidates}")

def main():
    print("=" * 70)
    print("🛒 [Part 2 / Q6] 인도 식료품 가격 Kafka Producer 가동")
    print(f"• 토픽: {TOPIC_NAME}")
    print(f"• 브로커: {BOOTSTRAP_SERVERS}")
    print("=" * 70)

    csv_path = find_csv_file()
    print(f"📁 CSV 데이터 로드: {csv_path}")

    records = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            record = {
                "item_name": row["item_name"].strip(),
                "market": row["market"].strip(),
                "year": int(row["year"]),
                "month": int(row["month"]),
                "price": float(row["price"]) if row["price"] else 0.0,
                "currency": row["currency"].strip(),
                "unit": row["unit"].strip(),
            }
            records.append(record)

    total_count = len(records)
    print(f"✅ CSV 파싱 완료: 총 {total_count}건 (목표 기준치: 200건)")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    sent_count = 0
    partition_counts = {0: 0, 1: 0, 2: 0}

    try:
        print()
        print("📡 product-prices 토픽으로 배치 전송 시작...")
        for idx, item in enumerate(records, start=1):
            if idx <= 68:
                target_p = 0
            elif idx <= 136:
                target_p = 1
            else:
                target_p = 2

            future = producer.send(topic=TOPIC_NAME, partition=target_p, value=item)
            md = future.get(timeout=10)
            p_id = md.partition
            partition_counts[p_id] += 1
            sent_count += 1

            if idx % 50 == 0 or idx == total_count:
                print(f"  [{idx:3d}/{total_count}] Market='{item['market']:12s}' Item='{item['item_name']:20s}' -> P:{p_id}")
            time.sleep(0.005)

    except Exception as e:
        print(f"❌ 전송 오류: {e}")
        raise
    finally:
        print()
        print("🧹 Kafka Producer 자원 정리 중 (flush & close)...")
        producer.flush()
        producer.close()
        print("✅ Producer 안전하게 종료 완료.")

    print()
    print("=" * 70)
    print(f"🎉 전송 완료 요약: 총 {sent_count}건 전송 성공 (목표: 200건)")
    print("📊 파티션별 적재 현황:")
    for p_id in sorted(partition_counts.keys()):
        print(f"  • Partition {p_id}: {partition_counts[p_id]}건")
    print("=" * 70)

if __name__ == "__main__":
    main()
