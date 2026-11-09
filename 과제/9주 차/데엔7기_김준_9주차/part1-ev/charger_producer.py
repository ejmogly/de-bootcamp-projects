#!/usr/bin/env python3
"""
[과제 9주차 - 문제 2]
충전 세션 Producer (실측 로그 replay + Producer 심화)

- 대상 데이터: charger_sessions.csv (175건 실측 세션)
- 정렬: startTime 기준 오름차순 정렬 후 시간차(0.05초) 순차 replay 전송
- 필수 Producer 옵션 3종:
    1) acks='all'
    2) enable_idempotence=True (멱등성 보장)
    3) 파티션 키: stationName (동일 충전소는 동일 파티션 보장)
- 메시지 스키마: JSON 형식 (10개 필드)
- 자원 관리: try...finally 블록에서 flush() 및 close() 보장
"""

import os
import csv
import json
import time
from kafka import KafkaProducer
import warnings
warnings.filterwarnings('ignore')

TOPIC_NAME = "ev-charging-sessions"
BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]
CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), "charger_sessions.csv")
REPLAY_DELAY_SEC = 0.05


def load_and_sort_sessions(file_path):
    sessions = []
    with open(file_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            session = {
                "stationName": str(row["stationName"]),
                "portNumber": int(row["portNumber"]),
                "portType": str(row["portType"]),
                "plugType": str(row["plugType"]),
                "startTime": str(row["startTime"]),
                "endTime": str(row["endTime"]),
                "totalDuration": str(row["totalDuration"]),
                "chargingDuration": str(row["chargingDuration"]),
                "energyKwh": float(row["energyKwh"]),
                "endedBy": str(row["endedBy"]),
            }
            sessions.append(session)
    sessions.sort(key=lambda x: x["startTime"])
    return sessions


def main():
    print("=" * 70)
    print("🚀 [Q2] 전기차 충전 세션 Kafka Producer 시작")
    print(f"• 토픽: {TOPIC_NAME}")
    print(f"• 브로커: {BOOTSTRAP_SERVERS}")
    print("=" * 70)

    candidate_paths = [
        CSV_FILE_PATH,
        os.path.join(os.path.dirname(__file__), "..", "data", "charger_sessions.csv"),
        os.path.join(os.path.dirname(__file__), "data", "charger_sessions.csv"),
        os.path.join("data", "charger_sessions.csv"),
        "charger_sessions.csv",
    ]
    csv_path = None
    for p in candidate_paths:
        if os.path.exists(p):
            csv_path = p
            break
    if not csv_path:
        raise FileNotFoundError(f"charger_sessions.csv 파일을 찾을 수 없습니다: {candidate_paths}")

    sessions = load_and_sort_sessions(csv_path)
    print(f"✅ CSV 데이터 로드 및 startTime 정렬 완료: 총 {len(sessions)}건 (기준 정답: 175건)")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    sent_count = 0
    partition_counts = {}

    try:
        print()
        print("📡 실측 로그 시간차 Replay 전송 시작...")
        for idx, session in enumerate(sessions, start=1):
            station_key = session["stationName"]

            future = producer.send(
                topic=TOPIC_NAME,
                key=station_key,
                value=session,
            )
            
            record_metadata = future.get(timeout=10)
            p_id = record_metadata.partition
            offset = record_metadata.offset
            partition_counts[p_id] = partition_counts.get(p_id, 0) + 1
            sent_count += 1

            if idx % 25 == 0 or idx == len(sessions):
                print(f"  [{idx:3d}/{len(sessions)}] Key='{station_key[:22]:22s}' -> P:{p_id}, Offset:{offset}")

            time.sleep(REPLAY_DELAY_SEC)

    except Exception as e:
        print(f"❌ 전송 중 오류 발생: {e}")
        raise
    finally:
        print()
        print("🧹 Kafka Producer 자원 정리 중 (flush & close)...")
        producer.flush()
        producer.close()
        print("✅ Kafka Producer 안전하게 종료 완료.")

    print()
    print("=" * 70)
    print(f"🎉 전송 완료 요약: 총 {sent_count}건 전송 성공 (목표: 175건)")
    print("📊 파티션별 분산 저장 현황 (stationName 해싱):")
    for p_id in sorted(partition_counts.keys()):
        print(f"  • Partition {p_id}: {partition_counts[p_id]}건")
    print("=" * 70)


if __name__ == "__main__":
    main()