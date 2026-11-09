# Problem 5: Kafka 브로커 장애 대응 및 복구 인시던트 보고서 (Incident Report)

## 1. 인시던트 개요
- **장애 대상:** Kafka 클러스터 내 브로커 ID 2 (`kafka-2`)
- **장애 유형:** 브로커 노드 비정상 종료 (Hard Failure)
- **클러스터 정책:** `default.replication.factor=2`, `min.insync.replicas=2`, Producer `acks=all`
- **장애 발생 시각:** **2026-10-14 15:59:15 UTC**
- **장애 복구 시각:** **2026-10-14 16:02:23 UTC**
- **복구 소요 시간:** **3분 08초** (클러스터 재동기화 및 URP=0 복귀까지 총 3분 40초)

---

## 2. 관측 메트릭 및 이상 징후 실측값

| 메트릭 / 항목 | 장애 전 (Normal) | 장애 중 (During Failure) | 복구 후 (Recovered) |
| :--- | :---: | :---: | :---: |
| **Grafana UnderReplicatedPartitions** | **0** | **57** (Spike 발생) | **0** (완전 정상화) |
| **클러스터 URP 파티션 행 수** | 0 개 | **57 개** (`--under-replicated-partitions`) | 0 개 (출력 없음) |
| **Active Controller** | broker 1 (1.0) | broker 1 (1.0 유지) | broker 1 (1.0 유지) |
| **`sensor-readings` ISR 상태** | 모든 파티션 ISR=2 | P0, P2, P3, P5 ISR=1 / P1, P4 ISR=2 | 모든 파티션 ISR=2 |

---

## 3. 장애 중 600건 메시지 전송 실측 결과표 (`fault_producer.py`)

- **전송 조건:** `acks=all`, `retries=0`, `request_timeout_ms=3000`, 6개 파티션 라운드로빈 100건씩 전송

| Partition | Sent | Success | Failed | Offset Delta | Exception Class | 복제본 배치 (Replicas ➔ ISR) |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Partition 0** | 100 | 0 | 100 | 0 | `NotEnoughReplicasError` | [3, 2] ➔ Isr: [3] (크기 1 < min.isr=2) |
| **Partition 1** | 100 | 100 | 0 | +100 | `-` (정상 전송) | [1, 3] ➔ Isr: [1, 3] (크기 2 == min.isr=2) |
| **Partition 2** | 100 | 0 | 100 | 0 | `NotEnoughReplicasError` | [2, 1] ➔ Isr: [1] (크기 1 < min.isr=2) |
| **Partition 3** | 100 | 0 | 100 | 0 | `NotEnoughReplicasError` | [3, 2] ➔ Isr: [3] (크기 1 < min.isr=2) |
| **Partition 4** | 100 | 100 | 0 | +100 | `-` (정상 전송) | [1, 3] ➔ Isr: [1, 3] (크기 2 == min.isr=2) |
| **Partition 5** | 100 | 0 | 100 | 0 | `NotEnoughReplicasError` | [2, 1] ➔ Isr: [1] (크기 1 < min.isr=2) |
| **합계 (Total)**| **600** | **200** | **400** | **+200** | - | 성공률: **33.3%** / 실패율: **66.7%** |

---

## 4. 서비스 영향 분석 및 기술적 근거

1. **CAP 이론 상 Consistency(일관성) 우선 선택:**
   - 카프카 클러스터에 `min.insync.replicas=2`와 프로듀서 `acks=all`이 강제되어 있으므로, 데이터 유실을 방지하기 위해 최소 2대의 브로커가 메시지를 안전하게 디스크에 써야만 성공 응답을 반환합니다.
2. **파티션별 영향도 차이의 근거:**
   - **Partition 1, 4:** Replicas가 `[1, 3]`으로 구성되어 장애 대상인 `kafka-2`를 포함하지 않았습니다. 따라서 ISR이 축소되지 않고 `[1, 3]` (크기 2)를 유지하여 쓰기 요청 100건이 모두 정상 커밋되었습니다.
   - **Partition 0, 2, 3, 5:** Replicas에 `kafka-2`가 포함되어 있어, 브로커 다운 즉시 ISR에서 2번 브로커가 축소(Shrink)되어 ISR 크기가 1로 감소했습니다. 살아있는 리더(1번 또는 3번)가 존재하더라도 `min.insync.replicas(2)` 조건을 충족할 수 없으므로 브로커는 쓰기 거부 에러(`NotEnoughReplicasException`)를 발생시켰습니다.
3. **최종 영향도 판정:**
   - 전체 파티션 중 66.7%(4/6)에서 쓰기 장애가 발생하였으며, 데이터 유실은 0건으로 일관성이 철저히 보장되었습니다.

---

## 5. 복구 후 10분간 리더 분포 추이 (`leader_timeline.png`)

| 시각 (UTC) | 경과 시간 | URP 수치 | Broker 1 Leader 수 | Broker 2 Leader 수 | Broker 3 Leader 수 | 상태 설명 |
| :---: | :---: | :---: | :---: | :---: | :---: | :--- |
| **16:02:25** | +0분 | 57 | 4 | 0 | 2 | `kafka-2` 컨테이너 시작 직후 |
| **16:03:25** | +1분 | 0 | 4 | 0 | 2 | ISR 복제 동기화 완료, URP=0 복귀 |
| **16:04:25** | +2분 | 0 | 4 | 0 | 2 | 브로커 2 팔로워 상태 유지 |
| **16:05:25** | +3분 | 0 | 4 | 0 | 2 | 리더 불균형 상태 유지 (1: 4개, 3: 2개) |
| **16:06:25** | +4분 | 0 | 4 | 0 | 2 | 자동 리밸런스 대기 |
| **16:07:25** | +5분 | 0 | 2 | 2 | 2 | **Preferred Leader Rebalance 주기 도래 (균등 분배 2:2:2)** |
| **16:08:25** | +6분 | 0 | 2 | 2 | 2 | 정상 균등 분배 유지 |
| **16:09:25** | +7분 | 0 | 2 | 2 | 2 | 정상 균등 분배 유지 |
| **16:10:25** | +8분 | 0 | 2 | 2 | 2 | 정상 균등 분배 유지 |
| **16:11:25** | +9분 | 0 | 2 | 2 | 2 | 정상 균등 분배 유지 |
| **16:12:25** | +10분 | 0 | 2 | 2 | 2 | 클러스터 완전 안정화 |
