#!/usr/bin/env python3
"""
[과제 9주차 - 문제 3]
Kafka Consumer 그룹 · 오프셋 · 리밸런싱

- 순수 Kafka Consumer (kafka-python 라이브러리 사용)
- Consumer Group ID: ev-consumer-group
- 오프셋 직접 관리: enable_auto_commit=False, 수동 consumer.commit()
- 소비하면서 endedBy별 카운트 집계 및 실시간 출력
- 컨슈머 2개 인스턴스 동시 실행 시 파티션 자동 리밸런싱 관찰
"""

import sys
import time
import json
import signal
from collections import Counter
from kafka import KafkaConsumer

TOPIC_NAME = "ev-charging-sessions"
GROUP_ID = "ev-consumer-group"
BOOTSTRAP_SERVERS = ["localhost:19092", "localhost:19093", "localhost:19094"]

ended_by_counter = Counter()
total_consumed = 0
running = True


def handle_sigint(sig, frame):
    global running
    print("\n[!] 종료 신호 수신. 안전하게 컨슈머를 종료합니다...")
    running = False


signal.signal(signal.SIGINT, handle_sigint)


def main():
    consumer_name = sys.argv[1] if len(sys.argv) > 1 else "Consumer-1"

    print("=" * 70)
    print(f"🚀 [Q3] {consumer_name} 시작 (Consumer Group: {GROUP_ID})")
    print(f"• 대상 토픽: {TOPIC_NAME}")
    print(f"• 오프셋 관리: 수동 커밋 (enable_auto_commit=False)")
    print("=" * 70)

    consumer = KafkaConsumer(
        TOPIC_NAME,
        group_id=GROUP_ID,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"✅ {consumer_name} 그룹({GROUP_ID}) 참여 완료! 할당된 파티션 수신 대기...")

    global total_consumed, running
    idle_count = 0

    try:
        while running:
            # 배치 폴링 (1초 타임아웃)
            records_dict = consumer.poll(timeout_ms=1000)
            if not records_dict:
                idle_count += 1
                if idle_count == 3 and total_consumed > 0:
                    print(f"\n[{consumer_name}] ☕ 현재까지 할당 파티션 메시지 소비 완료 (소비량: {total_consumed}건)")
                    print(f"[{consumer_name}] 📌 Kafka UI 'Members: 2' 캡처를 위해 그룹 연결 유지 중... (종료: Ctrl+C)")
                time.sleep(0.5)
                continue

            idle_count = 0
            for tp, records in records_dict.items():
                for record in records:
                    val = record.value
                    ended_by = val.get("endedBy", "Unknown")
                    ended_by_counter[ended_by] += 1
                    total_consumed += 1

                    # 수동 오프셋 커밋 (오프셋 직접 관리)
                    consumer.commit()

                    print(f"[{consumer_name}] [P:{record.partition}|Offset:{record.offset:2d}] "
                          f"Station='{val.get('stationName', '')[:18]:18s}' | endedBy='{ended_by}'")

    except Exception as e:
        print(f"❌ 소비 중 오류: {e}")
    finally:
        consumer.close()
        print(f"\n✅ {consumer_name} 안전 종료 완료.")

    # 최종 집계 결과 출력
    print("=" * 70)
    print(f"📊 [{consumer_name}] 최종 집계 결과 (총 소비: {total_consumed}건):")
    for reason, count in ended_by_counter.most_common():
        print(f"  • {reason}: {count}건")
    print("=" * 70)


if __name__ == "__main__":
    main()
