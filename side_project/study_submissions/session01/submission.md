# 1회차 복습 과제 양식

이 파일은 수업 중 작성하지 않고 수업 후 복습에 사용합니다. 제출 방법과 기한은 수업 마무리의 강사 안내를 따릅니다.

이름 / 학생 ID: 김준 (junekim) / de_bootcamp_7th
작성일: 2026-10-23

공통 orders·payments·refunds 예제의 예측과 수집 결과를 다시 살펴볼 수 있습니다. 개인 도메인·테이블·DDL 설계는 3회차에 진행합니다.

## 1. 왜 CDC 레이크하우스인가

- 전체 복사나 updated_at 조회보다 로그 기반 CDC가 필요한 상황 한 가지:
  물리적 삭제(Hard DELETE)가 일어나는 테이블에서는 `updated_at` 폴링 방식으로는 삭제 사실 자체를 감지할 수 없으며, 데이터가 수천만 건으로 증가할 때 주기적인 Full Scan 폴링은 원천 DB에 심각한 I/O 부하와 락(Lock)을 유발합니다. 반면 로그 기반 CDC(Debezium)는 DB의 WAL(Write-Ahead Log)을 직접 읽으므로 원천 DB에 부하 없이 DELETE를 포함한 모든 변경 이력(before/after)을 실시간으로 완벽하게 감지할 수 있습니다.

- CDC를 선택해도 관리해야 하는 비용·운영 문제 한 가지:
  Kafka 컨슈머 지연(Lag)이나 다운스트림 적재 엔진(Flink/Iceberg) 장애 발생 시 원천 DB의 복제 슬롯(Replication Slot)에 WAL 로그가 계속 누적되어 원천 DB 디스크 풀(Disk Full) 장애가 발생할 수 있으므로, 복제 슬롯 지연 모니터링 및 Schema Evolution(스키마 변경 대응)에 대한 엄격한 운영 관리가 필요합니다.

## 2. Iceberg와 아키텍처 역할 정리

| 구간·구성 요소 | 내 말로 한 줄 |
|---|---|
| PostgreSQL → Debezium → Kafka | PostgreSQL WAL의 모든 변경(c/u/d)을 Debezium이 논리적 복제하여 JSON Envelope 이벤트로 변환 후 Kafka 토픽에 순서대로 버퍼링하는 구간 |
| Flink → Iceberg Bronze | Kafka의 원문 이벤트 payload를 실시간으로 스트리밍 읽어와 Tombstone은 필터링하고 MinIO의 Iceberg Bronze 테이블에 Parquet 파일로 Append 적재하는 구간 |
| MinIO / JDBC Catalog / Flink 복구 상태 | 실제 Parquet 데이터 및 Iceberg 메타데이터 파일(JSON/manifest)을 보관하는 S3 스토리지(MinIO), 최신 테이블 메타데이터 포인터를 관리하는 PostgreSQL(JDBC Catalog), 장애 복구를 위한 체크포인트 저장소 |
| Spark SQL Silver / dbt Gold — 후속 회차 | Bronze의 변경 로그를 최신 상태로 MERGE/Upsert하여 Silver 정제 테이블을 만들고, dbt로 비즈니스 지표(정산 마트 등)를 빌드하여 Gold 계층을 구축하는 분석 구간 |

- Bronze·Silver·Gold와 Iceberg 내부 메타데이터 구조의 차이 한 문장:
  Bronze·Silver·Gold는 비즈니스 정제 수준과 파이프라인 흐름을 나타내는 논리적 데이터 레이크하우스 아키텍처 계층이며, Iceberg 내부 메타데이터는 ACID 트랜잭션과 시점 조회(Time-travel)를 보장하기 위해 'Table Metadata JSON → Manifest List (Avro) → Manifest File (Avro) → Data File (Parquet)' 계층 구조로 파일 경로와 통계를 추적하는 물리적 메타데이터 명세입니다.

## 3. 환경 확인과 ingest.sql 읽기

- 개인 환경 구성과 컨테이너 기동 결과·캡처:
  9개 컨테이너(source-db, catalog-db, kafka, connect, minio, minio-init, jobmanager, taskmanager, runner) 모두 정상 구동 및 healthy 상태 확인 완료.
  ```text
  NAME                      IMAGE                                  COMMAND                  SERVICE       STATUS
  cdc-study-catalog-db-1    postgres:17.6-alpine                   "docker-entrypoint.s…"   catalog-db    Up (healthy)
  cdc-study-connect-1       quay.io/debezium/connect:3.2.4.Final   "/docker-entrypoint.…"   connect       Up
  cdc-study-jobmanager-1    cdc-study-flink:1.20.4-iceberg1.10.0   "/docker-entrypoint.…"   jobmanager    Up
  cdc-study-kafka-1         apache/kafka:3.9.1                     "/__cacert_entrypoin…"   kafka         Up (healthy)
  cdc-study-minio-1         cgr.dev/chainguard/minio:latest        "/usr/bin/minio serv…"   minio         Up (healthy)
  cdc-study-runner-1        cdc-study-runner                       "python scripts/runn…"   runner        Up
  cdc-study-source-db-1     postgres:17.6-alpine                   "docker-entrypoint.s…"   source-db     Up (healthy)
  cdc-study-spark-1         cdc-study-spark                        "bash /opt/start-thr…"   spark         Up
  cdc-study-taskmanager-1   cdc-study-flink:1.20.4-iceberg1.10.0   "/docker-entrypoint.…"   taskmanager   Up
  ```
- 개인 환경에서 확인한 preflight ready 상태: true
  ```json
  {
    "ready": true,
    "vm_free_gib": 404.14,
    "job_id": "81ab67c9e653a0339698bd1043212363",
    "row_counts": {
      "lake.bronze.cdc_events": 2,
      "lake.silver.payments": 1,
      "lake.gold.fct_order_settlement": 1
    },
    "checkpoint_counts": {
      "completed": 9,
      "failed": 0
    }
  }
  ```
- student-worksheet.sql에서 확인한 내용이나 남은 질문:
  - Flink SQL의 `JdbcCatalog` 및 raw 포맷 설정의 역할을 확인하였고, [B] 검증 SELECT 및 [C] CDC 이벤트 예측 작성을 완료했습니다.
- SQL [A]에 Catalog·raw 포맷·checkpoint·null 제외 조건의 역할을 정리했습니다: 예
- Flink RUNNING이나 checkpoint 성공만으로 수집 완료라고 할 수 없는 이유:
  Flink Job의 Checkpoint가 성공했더라도, Iceberg Sink가 S3(MinIO)에 기록된 데이터 파일들을 바탕으로 신규 스냅샷 메타데이터를 작성하고 JDBC Catalog에 트랜잭션 커밋(commit)을 최종 완료해야 비로소 외부 쿼리 엔진(Spark, Trino 등)에서 조회할 수 있기 때문입니다. 즉, Checkpoint 완료 후 Catalog Commit 및 실제 외부 조회까지 확인되어야 진정한 수집 완료입니다.

## 4. 공통 예제 사전 예측과 수집 결과 대조

개인 실행 CASE_ID: s01-junekim-run
실제 환불 ID — CASE_ID-r1: s01-junekim-run-r1
공통 토픽 / 파티션: cdc.public.refunds / partition 0 (개인 실행에서 확인)
비교한 자료: 개인 실행

개인 실행의 예측과 실제 before/after·offset·copies를 아래 표에서 비교합니다. 강사 시연이나 과거 리허설 자료를 참고한 값은 개인 실행 결과와 구분합니다. 확인하지 못한 값은 미확인으로 남기고 추정하지 않습니다. 전체 JSON을 옮길 필요는 없습니다.

| 변경(op) | 기대 원천 행 / before→after amount | 실제 before→after | Kafka offset | bronze_copies |
|---|---|---|---|---|
| INSERT(c) | 1행 / null → 20000 | null → 20000 | 4 | 1 |
| UPDATE(u) | 1행 / 20000 → 30000 | 20000 → 30000 | 5 | 1 |
| DELETE(d) | 0행 / 30000 → null | 30000 → null | 6 | 1 |

### 다시 살펴볼 질문

- Kafka key의 refund_id와 원천 PK: 일치 (`s01-junekim-run-r1`)
- 원천의 환불 행: INSERT 뒤 1행·20000 / UPDATE 뒤 1행·30000 / DELETE 뒤 0행 확인 여부: 확인 완료
- Kafka와 Bronze의 같은 topic·partition·offset 및 내용: 일치
- DELETE 뒤 이번 환불 키의 Bronze history_ops: c,u,d
- 이번 환불 키의 Kafka null tombstone 수: 1 (offset 7)
- tombstone을 제외한 Bronze 이력 행 수: 3 (offset 4, 5, 6)
- 원천은 0행인데 Bronze에는 c/u/d가 남는 이유:
  PostgreSQL의 DELETE는 원천 DB에서 해당 행을 물리적으로 삭제하여 0행이 되지만, Debezium이 WAL 로그로부터 삭제 직전 값(`before.amount=30000`)을 담은 `op='d'` 이벤트를 캡처하여 Kafka에 발행했고, Flink가 이 이벤트를 불변(Immutable) Append-only 로그 형태로 Iceberg Bronze에 영구 저장하였기 때문에 원천 삭제 후에도 전체 변경 라이프사이클(c, u, d) 이력이 Bronze에 온전히 보존됩니다.

### DELETE 요약의 지연 한 세트

| kafka_ts | ingested_at | wait_observed_seconds |
|---|---|---|
| 2026-10-23 13:20:42.152000 | 2026-10-23 13:20:42.229000 | 8.51 |

이 값만으로 원천 commit부터 분석 마트 반영까지의 전체 지연을 계산할 수 없는 이유:
위 지표들은 원천 DB 커밋 시각(source_ts_ms)이 아닌 Kafka 도착 시각(kafka_ts), Flink 처리 시각(ingested_at), 그리고 wait 명령 실행 후 Bronze 조회까지의 관찰 시간(wait_observed_seconds)만을 측정한 것이며, Iceberg Checkpoint 주기, JDBC Catalog Commit 지연 시간, 그리고 후속 배치인 Spark SQL(Silver MERGE) 및 dbt(Gold Mart) 실행 주기가 포함되어 있지 않기 때문입니다.

Kafka에 있으나 Bronze에 없을 때 먼저 확인할 구간 한 문장:
Flink 스트리밍 작업의 토픽 패턴 일치 여부(`topic-pattern`), Flink Job 상태(RUNNING/FAILING) 및 Checkpoint 주기, 그리고 Iceberg Sink의 S3 스토리지(MinIO) 및 Catalog DB 접근 권한/커밋 성공 여부를 먼저 확인해야 합니다.

## 5. 복습 점검과 2회차 질문

- [x] 개인 환경 구성과 컨테이너 기동 결과·캡처 정리
- [x] 공통 환불 INSERT·UPDATE·DELETE 실행과 결과·막힌 지점 기록
- [x] SQL [A] 네 가지 설정의 역할 정리
- [x] SQL [B] UPDATE·DELETE 뒤 같은 PK를 조회하는 SELECT 작성
- [x] SQL [C] 공통 환불 c/u/d 이벤트 예측
- [x] 예측과 실제 수집 결과의 차이 확인 — 미확인 값은 추정하지 않기

다음 회차에서 확인하고 싶은 수집 설정·동작 질문 한 가지:
실무에서 대용량 트래픽으로 인해 Kafka 파티션이 여러 개로 분할될 경우, 동일 PK에 대한 순서 보장(Key Partitioning)을 유지하면서도 Flink-Iceberg 싱크에서 Out-of-Order(순서 뒤바뀜) 이벤트를 어떻게 정합성 있게 처리하는지 궁금합니다.

관심 도메인 메모 (선택, 설계 과제 아님):
이커머스 실시간 주문-결제-정산 파이프라인 및 물류 배송 상태 추적 도메인

2회차는 PostgreSQL → Debezium → Kafka 수집 심화입니다. 개인 프로젝트 설계 확정은 3회차에 진행하며, 오늘 개인 DDL·개인 DML은 작성하지 않습니다. 프로젝트 발표는 8회 스터디 뒤 별도 일정입니다.
