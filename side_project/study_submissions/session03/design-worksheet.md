# 3회차 설계 학습지

공통 예제를 함께 보며 한 줄씩 적고, 수업 말미 개인 작업 30분에 각자의 도메인·원천 테이블·적재할 정보를 확정합니다.

이름 / 학생 ID: 김준 (junekim) / de_bootcamp_7th
프로젝트명: 스마트 퀵커머스 실시간 주문·물류·정산 레이크하우스 파이프라인

## 1. 도메인과 업무 질문

- 도메인:
  도심형 즉시 배송(15~30분 마이크로 풀필먼트)을 지원하는 **스마트 퀵커머스(Quick-Commerce) 플랫폼**
- 결과를 사용할 사람과 판단:
  - 재무/정산팀: 결제 승인, 부분 취소, 결제 철회에 따른 PG 수수료 및 일일 정산 대사 정합성 판단
  - 물류/운영팀: 라이더 배차 후 픽업/배송 완료까지의 실시간 리드타임 모니터링 및 30분 SLA 초과 지연 건 페널티/보상 판단
- 수정·삭제가 늦거나 잘못 반영됐을 때 생기는 일:
  - 고객의 결제 취소(Hard DELETE)가 정산 마트에 반영되지 못해 가맹점/라이더에게 과오지급 정산 사고 발생
  - 라이더의 배송 완료 상태가 늦게 반영되어 동일 주문에 대한 중복 배차 및 CS 컴플레인 급증
- 답하려는 업무 질문 한 문장:
  원천 DB에서 결제 내역이 취소·삭제되더라도 변경 전 금액을 온전히 보존하여 회계 정산 누수를 방지하고, 실시간 배송 상태 변경을 추적하여 30분 이내 SLA 준수율을 100% 감시할 수 있는가?

## 2. 서로 다른 두 지연 목표

| 범위 | 잠정 목표 | 필요한 이유 | 아직 확인할 것 |
|---|---|---|---|
| 원천 변경부터 Bronze 조회 | 5초 ~ 10초 이내 (실측: 6.56초) | 결제 철회 및 배송 상태 전이 이벤트가 발생하는 즉시 불변 Parquet 로그로 유실 없이 격리 보존되어야 함 | Flink Checkpoint 주기(10초) 및 MinIO S3 I/O 부하 시 지연 변동폭 |
| 원천 변경부터 최종 지표 | 1분 ~ 3분 이내 (준실시간 마이크로배치) | 운영 대시보드(Gold Mart)에서 실시간 배송 지연 라이더 감지 및 당일 취소 정산 합계 모니터링 필요 | Spark SQL의 Silver MERGE 주기 및 dbt 빌드 소요 시간 |

## 3. 원천 테이블과 변경 — 오늘 확정

| 테이블 | 한 행의 의미 | PK·FK | 시간·순서 판단 정보 | 이벤트 유형 | 삭제 정책 |
|---|---|---|---|---|---|
| ecommerce.orders | 고객 1건의 주문 마스터 헤더 | PK: order_id | ordered_at, updated_at | INSERT, UPDATE | 상태 컬럼 전이 (소프트 삭제) |
| ecommerce.payments | 주문에 대한 결제 승인/부분환불/철회 | PK: payment_id, FK: order_id | paid_at | INSERT, UPDATE, DELETE | 취소 시 원천 물리 삭제 (Hard DELETE) |
| ecommerce.deliveries | 라이더 배차 및 실시간 배송 이동 상태 | PK: delivery_id, FK: order_id | dispatched_at, delivered_at | INSERT, UPDATE | 상태 컬럼 전이 (DELIVERED/CANCELLED) |

| 변경 사례 | 바꾸는 키·값 | 변경 뒤 기대하는 업무 상태 |
|---|---|---|
| 신규 (INSERT) | orders(order_id='ORD-01', total_amount=45000), payments(pay_amount=45000) | 신규 주문 접수 및 결제 승인 완료 상태 |
| 수정 (UPDATE) | payments(pay_amount=38000, status='PARTIAL_REFUNDED'), deliveries(status='IN_TRANSIT') | 품절 품목 부분 환불 반영 및 라이더 배송 출발 상태 |
| 삭제 (DELETE) | DELETE FROM payments WHERE payment_id='PAY-01' (결제 철회) | 원천에서는 레코드 삭제(0행)되나, Bronze에는 before=38000 보존 |

## 4. 수집·처리 방식과 선택 이유

- 내 요구에서 배치·수정 시각 증분·로그 CDC를 비교한 결과:
  배송 상태(수분 단위)와 결제 취소(DELETE)를 다루므로, updated_at 폴링 방식은 삭제 감지가 불가능하고 DB 부하를 유발함. 따라서 WAL 로그 기반 Debezium CDC가 유일한 해법임.
- Kafka를 둘 이유와 보관·운영 비용:
  피크 타임(점심/저녁 주문 폭주) 시 트래픽 스파이크를 완충하는 버퍼링 역할을 하며, 후속 적재 엔진 장애 시에도 오프셋 기반 안전 재처리가 가능함.
- Flink를 선택한 이유 또는 아직 확인할 이유:
  밀리초 단위 스트리밍 처리로 Kafka 토픽의 원문 JSON을 실시간 파싱하고, Iceberg Sink와 연동하여 Exactly-once 체크포인트를 보장하며 Parquet 파일로 직접 커밋 가능.
- 비교한 대안 하나:
  Spark Structured Streaming (마이크로배치 기반이어서 초단위 지연 및 체크포인트 오버헤드가 상대적으로 큼)
- 내가 감수할 운영 부담 한 가지:
  Flink JobManager/TaskManager 클러스터 리소스 모니터링 및 카프카 토픽의 Schema Evolution(스키마 진화) 대응 운영 부담.

## 5. 입력·Bronze와 후속 계층의 책임 — 오늘 확정

| 계층 또는 구성 | 내 설계에서 맡길 질문·이유 |
|---|---|
| Kafka 토픽 | cdc.ecommerce.orders, cdc.ecommerce.payments, cdc.ecommerce.deliveries (Key: order_id 기반 파티셔닝) |
| Bronze 계층 | Flink로 format='raw' 보존. 원천에서 DELETE된 건도 op='d'와 before.amount를 불변 Parquet 파일로 영구 보존 |
| Silver 계층 | Spark SQL MERGE INTO를 통해 각 테이블의 최신 레코드를 Upsert 유지하고, DELETE 건은 is_deleted=TRUE로 논리 보존 |
| Gold 계층 | dbt 모델을 통해 [주문-결제 대사 일치율], [라이더별 30분 SLA 준수율 및 정산 마트] 팩트/디멘전 테이블 생성 |

- 후속 Silver·Gold에서 테이블을 연결·집계할 키 한 줄:
  orders.order_id = payments.order_id 및 orders.order_id = deliveries.order_id

## 6. 내부 동작과 관찰에서 남길 판단

- Flink가 하는 일과 후속 Silver가 할 일:
  Flink는 Kafka 메시지 원문(payload)을 손실 없이 Iceberg Bronze에 빠른 주기로 Append 적재하며, Silver는 이 이력을 읽어 PK 기준으로 최신 상태를 머지(MERGE)함.
- 실제 행·metadata 파일·현재 metadata 위치가 각각 있는 곳:
  - 실제 행 데이터: MinIO 버킷 (s3://warehouse/bronze/.../*.parquet)
  - 메타데이터 트리: MinIO 버킷 (s3://warehouse/bronze/.../metadata/*.metadata.json)
  - 현재 테이블 메타데이터 포인터: PostgreSQL catalog-db (iceberg_tables 테이블)
- MinIO에서 본 파일 경로와 SQL 결과를 연결한 근거:
  Spark SQL의 _file 가상 컬럼 및 lake.bronze.cdc_events.files 메타데이터 조회의 file_path
- 장애가 의심될 때 먼저 볼 증거와 그 이유:
  Flink Web UI의 Checkpoint 카운트 증가 여부 및 실패 횟수. 체크포인트가 돌지 않으면 Iceberg 테이블에 신규 스냅샷 커밋이 이루어지지 않기 때문.

## 7. 현재 상태·재전달의 규칙 초안

- 현재 상태 한 건을 묶는 키: order_id (주문), payment_id (결제), delivery_id (배송)
- 삭제·취소를 반영하는 방법: Silver 계층에서 op='d' 감지 시 물리 삭제 대신 is_deleted = TRUE, deleted_at = source_ts로 마킹
- 늦거나 반복된 변경에서 최신 상태를 판단할 정보: source.ts_ms (원천 트랜잭션 커밋 타임스탬프) 및 source.lsn
- 같은 변경을 다시 받았을 때 그대로여야 하는 값과 기대값: 멱등성(Idempotency) 유지로 인해 이미 반영된 LSN 이하의 중복 이벤트는 무시되어 금액/상태가 중복 가산되지 않음.

## 8. 확정 내용과 리뷰

- 오늘 확정한 도메인·테이블·한 행·PK/FK·시간/순서·이벤트 유형·삭제 정책:
  스마트 퀵커머스 도메인의 3개 테이블(ecommerce.orders, ecommerce.payments, ecommerce.deliveries), REPLICA IDENTITY FULL 적용 및 실제 PostgreSQL 생성 검증 완료.
- 확정한 토픽·Bronze 보존 정보와 선택 이유:
  토픽 3개, Tombstone 제외(WHERE payload IS NOT NULL), 원문 Envelope 전체 보존
- 4–6회차 내 프로젝트의 Silver·Gold·수정/삭제·소급 정정 구현 전에 확인할 일:
  Spark SQL의 MERGE INTO 구문 작성, 주문과 결제 1:N 조인 정합성 검증, dbt 모델링 환경 구성
