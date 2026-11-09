# 2회차 복습 과제 — 원천 설정과 변경 이력 확인

이 양식은 수업 후 개인 환경에서 공통 예제를 실행하고 정리할 때 사용합니다. 수업 중 새로 작성하거나 제출하는 활동은 아닙니다. 제출 방법과 기한은 강사 안내를 따릅니다.

이름 / 학생 ID: 김준 (junekim) / de_bootcamp_7th
작성일: 2026-10-26
개인 실행 CASE_ID / 환불 ID: s01-junekim-01 / s01-junekim-01-r1

개인 실행·강사 시연·고정 fixture·과거 리허설 자료를 구분합니다. 직접 확인하지 못한 결과는 미확인으로 남기고, 막힌 경우 오류 화면·진행 지점·질문을 적습니다.

## 1. 이번 진행 상황

- 개인 환경 구성과 컨테이너 기동 결과:
  Docker 기반 9대 분산 서비스 컨테이너(source-db, catalog-db, kafka, connect, minio, jobmanager, taskmanager, runner, spark) 모두 정상 구동(healthy) 및 Flink cdc-bronze 스트리밍 작업 RUNNING 상태 유지 확인 완료.
  
- 지난 복습에서 이어서 해결한 것:
  Flink Job 기동 및 Spark Thrift Server(포트 10000) 바인딩 대기 지연 문제를 점검하여 preflight ready=true 상태를 확립하였고, 환불 생성(c) -> 정정(u) -> 삭제(d)의 3단계 라이프사이클을 개인 네임스페이스(s01-junekim-01)로 직접 완주했습니다.
- 아직 막힌 지점과 다음에 확인할 것:
  막힌 지점 없이 2차시 검증을 모두 완료하였으며, 다음에는 실시간 변경 데이터를 Silver 계층으로 정제하는 Spark MERGE 쿼리 및 dbt 모델링 파이프라인 연계를 확인하고자 합니다.

## 2. PostgreSQL 수집 조건

| 확인 항목 | 내 환경에서 확인한 값 / 미확인 | 역할을 내 말로 한 줄 |
|---|---|---|
| wal_level | logical | PostgreSQL에서 트랜잭션 커밋 로그를 논리 디코딩(Logical Decoding)하여 CDC 변경 스트림으로 추출할 수 있게 하는 필수 엔진 설정 |
| cdc_lab publication과 수집 테이블 | cdc_lab (orders, payments, refunds) | 논리 복제 대상 테이블들의 INSERT, UPDATE, DELETE, TRUNCATE 이벤트를 발행하는 PostgreSQL 발행자 객체 |
| refunds의 replica identity | f (FULL) | UPDATE/DELETE 발생 시 변경 전의 모든 컬럼 값(before 전체)을 WAL 로그에 기록하도록 보장하는 테이블 레벨 설정 |
| cdc_lab replication slot | cdc_lab (plugin: pgoutput, active: true) | Debezium 소비자의 LSN 진행 위치를 추적하고, 소비되기 전까지 WAL 세그먼트가 PostgreSQL에 의해 임의 삭제되지 않도록 보존하는 상태 저장소 |

- REPLICA IDENTITY FULL과 before.amount의 관계:
  기본값(DEFAULT)일 경우 UPDATE/DELETE 시 WAL에 변경 전 PK만 기록되므로 before.amount가 누락되지만, FULL로 설정되어 있어야 변경 직전 금액(20000 -> 30000 정정 시 before.amount=20000, DELETE 시 before.amount=30000)을 온전히 캡처할 수 있습니다.
- slot이 active여도 Bronze 반영 완료로 판단할 수 없는 이유:
  active=true는 Debezium 커넥터가 슬롯에 정상 연결되어 WAL 스트림을 읽고 있다는 의미일 뿐이며, Kafka 토픽 버퍼링, Flink 스트리밍 처리, 체크포인트 주기 완료, 그리고 Iceberg Sink의 MinIO S3 및 JDBC Catalog 트랜잭션 커밋까지 완료되어야 비로소 Bronze 조회가 가능하기 때문입니다.
- 환경 구성·위 조회 결과 캡처:
  {
  "settings": [
    {
      "name": "max_replication_slots",
      "setting": "10"
    },
    {
      "name": "max_wal_senders",
      "setting": "10"
    },
    {
      "name": "wal_level",
      "setting": "logical"
    }
  ],
  "slots": [
    {
      "slot_name": "cdc_lab",
      "plugin": "pgoutput",
      "slot_type": "logical",
      "database": "app",
      "active": true,
      "restart_lsn": "0/199A200",
      "confirmed_flush_lsn": "0/199A570"
    }
  ],
  "publications": [
    {
      "pubname": "cdc_lab",
      "pubinsert": true,
      "pubupdate": true,
      "pubdelete": true,
      "pubtruncate": true
    }
  ],
  "publication_tables": [
    {
      "pubname": "cdc_lab",
      "schemaname": "public",
      "tablename": "orders"
    },
    {
      "pubname": "cdc_lab",
      "schemaname": "public",
      "tablename": "payments"
    },
    {
      "pubname": "cdc_lab",
      "schemaname": "public",
      "tablename": "refunds"
    }
  ],
  "replica_identity": [
    {
      "schemaname": "public",
      "tablename": "orders",
      "replica_identity_code": "f"
    },
    {
      "schemaname": "public",
      "tablename": "payments",
      "replica_identity_code": "f"
    },
    {
      "schemaname": "public",
      "tablename": "refunds",
      "replica_identity_code": "f"
    }
  ],
  "primary_keys": [
    {
      "table_name": "orders",
      "column_name": "order_id",
      "ordinal_position": 1
    },
    {
      "table_name": "payments",
      "column_name": "payment_id",
      "ordinal_position": 1
    },
    {
      "table_name": "refunds",
      "column_name": "refund_id",
      "ordinal_position": 1
    }
  ],
  "replica_identity_legend": {
    "d": "DEFAULT",
    "f": "FULL",
    "i": "INDEX",
    "n": "NOTHING"
  },
  "scope": "Read-only PostgreSQL capture conditions; active slot alone does not prove delivery",
  "observed_at": "2026-10-25T23:38:56.313990+00:00"
} 명령을 통해 max_replication_slots=10, wal_level=logical, slot_name=cdc_lab, active=true, confirmed_flush_lsn=0/199A570 정상 확인.

## 3. 같은 환불의 c/u/d 따라가기

관찰 근거: 개인 실행 ()

| 변경 | 기대 원천 상태 | 실제 원천 상태 | Kafka op·before→after | Bronze 같은 수집 위치·내용 |
|---|---|---|---|---|
| INSERT 20000 | 1행·20000 | 1행 / 20,000원 | op=c, before=null, after=20000 | topic: cdc.public.refunds, partition: 0, offset: 8, copies: 1 |
| UPDATE 30000 | 1행·30000 | 1행 / 30,000원 | op=u, before=20000, after=30000 | topic: cdc.public.refunds, partition: 0, offset: 9, copies: 1 |
| DELETE | 0행 | 0행 (물리 삭제) | op=d, before=30000, after=null | topic: cdc.public.refunds, partition: 0, offset: 10, copies: 1 |

- 이번 환불의 Bronze op 목록과 이력 행 수:
  ['c', 'u', 'd'] 총 3건의 불변 감사 로그 행 보존.
- Kafka와 Bronze에서 대조한 topic·partition·offset:
   / partition 0 / offset 8, 9, 10 완벽 일치.
- after 조건만으로 조회하면 DELETE를 놓치는 이유:
  DELETE 이벤트에서는 원천 레코드가 사라져 after가 null로 발행되므로,  조건만 사용하면 해당 행이 필터링되어 삭제 이력이 누락됩니다. 반드시  조건을 OR로 포함해야 합니다.
- tombstone과 op=d의 차이:
  는 삭제 직전 데이터()를 포함하는 유효한 Debezium Envelope JSON 메시지이며, 은 카프카 로그 압축(Log Compaction)을 위해 DELETE 직후 발행되는 바이트 길이가 0인  레코드입니다.
- SQL [A] 조회 결과 캡처 또는 미확인 지점:
  개인 실행 에 대해 Spark SQL로 3행 모두 정상 조회 완료 ().

## 4. 테이블에서 실제 파일까지

- Iceberg 파일 목록에서 확인한 Parquet 경로:
  1)  (c 이벤트)
  2)  (u 이벤트)
  3)  (d 이벤트)
- 직접 내용을 확인한 방법과 결과 / 미확인:
  {
  "namespace": "s01-junekim-01",
  "refund_id": "s01-junekim-01-r1",
  "bronze_records": [
    {
      "topic": "cdc.public.refunds",
      "partition_id": 0,
      "kafka_offset": 8,
      "payload": "{"before":null,"after":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":20000,"status":"success"},"source":{"version":"3.2.4.Final","connector":"postgresql","name":"cdc","ts_ms":1792970702843,"snapshot":"false","db":"app","sequence":"[\"26844424\",\"26844424\"]","ts_us":1792970702843205,"ts_ns":1792970702843205000,"schema":"public","table":"refunds","txId":770,"lsn":26844424,"xmin":null},"transaction":null,"op":"c","ts_ms":1792970703118,"ts_us":1792970703118632,"ts_ns":1792970703118632969}",
      "kafka_ts": "2026-10-25 23:25:03.296000",
      "ingested_at": "2026-10-25 23:25:03.317000",
      "parquet_file": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12948.parquet",
      "op": "c",
      "before_amount": null,
      "after_amount": 20000,
      "refund_id": "s01-junekim-01-r1",
      "source_lsn": 26844424
    },
    {
      "topic": "cdc.public.refunds",
      "partition_id": 0,
      "kafka_offset": 9,
      "payload": "{"before":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":20000,"status":"success"},"after":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":30000,"status":"success"},"source":{"version":"3.2.4.Final","connector":"postgresql","name":"cdc","ts_ms":1792970813039,"snapshot":"false","db":"app","sequence":"[\"26845320\",\"26845376\"]","ts_us":1792970813039276,"ts_ns":1792970813039276000,"schema":"public","table":"refunds","txId":771,"lsn":26845376,"xmin":null},"transaction":null,"op":"u","ts_ms":1792970813389,"ts_us":1792970813389354,"ts_ns":1792970813389354464}",
      "kafka_ts": "2026-10-25 23:26:53.733000",
      "ingested_at": "2026-10-25 23:26:53.755000",
      "parquet_file": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12959.parquet",
      "op": "u",
      "before_amount": 20000,
      "after_amount": 30000,
      "refund_id": "s01-junekim-01-r1",
      "source_lsn": 26845376
    },
    {
      "topic": "cdc.public.refunds",
      "partition_id": 0,
      "kafka_offset": 10,
      "payload": "{"before":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":30000,"status":"success"},"after":null,"source":{"version":"3.2.4.Final","connector":"postgresql","name":"cdc","ts_ms":1792970832561,"snapshot":"false","db":"app","sequence":"[\"26845608\",\"26845872\"]","ts_us":1792970832561839,"ts_ns":1792970832561839000,"schema":"public","table":"refunds","txId":772,"lsn":26845872,"xmin":null},"transaction":null,"op":"d","ts_ms":1792970832712,"ts_us":1792970832712512,"ts_ns":1792970832712512501}",
      "kafka_ts": "2026-10-25 23:27:12.823000",
      "ingested_at": "2026-10-25 23:27:12.843000",
      "parquet_file": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12961.parquet",
      "op": "d",
      "before_amount": 30000,
      "after_amount": null,
      "refund_id": "s01-junekim-01-r1",
      "source_lsn": 26845872
    }
  ],
  "history_ops": [
    "c",
    "u",
    "d"
  ],
  "record_count": 3,
  "scope": "Selected current append-only Bronze files were read as bytes and decoded; exact payload+Kafka identity matched SQL. This is not a general Iceberg delete-file reader or exactly-once guarantee.",
  "files": [
    {
      "file_path": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12948.parquet",
      "file_format": "PARQUET",
      "record_count": 3,
      "file_size_in_bytes": 3620
    },
    {
      "file_path": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12959.parquet",
      "file_format": "PARQUET",
      "record_count": 1,
      "file_size_in_bytes": 4780
    },
    {
      "file_path": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12961.parquet",
      "file_format": "PARQUET",
      "record_count": 1,
      "file_size_in_bytes": 4395
    }
  ],
  "metadata_location": "s3://warehouse/bronze/cdc_events/metadata/01370-f8fe2a59-0ecb-4095-b109-4803d2ba8345.metadata.json",
  "read_method": "S3FileIO physical bytes + Iceberg GenericParquetReaders; no table scan substitution",
  "physical_files": [
    {
      "file_path": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12948.parquet",
      "byte_count": 3620,
      "sha256": "22c5d491d04376f9aeb0b0de0e7e30555095707cadf5825f7315c83690e112b5",
      "magic_start": "PAR1",
      "magic_end": "PAR1",
      "decoded_record_count": 3,
      "matching_records": [
        {
          "topic": "cdc.public.refunds",
          "partition_id": 0,
          "kafka_offset": 8,
          "payload": "{"before":null,"after":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":20000,"status":"success"},"source":{"version":"3.2.4.Final","connector":"postgresql","name":"cdc","ts_ms":1792970702843,"snapshot":"false","db":"app","sequence":"[\"26844424\",\"26844424\"]","ts_us":1792970702843205,"ts_ns":1792970702843205000,"schema":"public","table":"refunds","txId":770,"lsn":26844424,"xmin":null},"transaction":null,"op":"c","ts_ms":1792970703118,"ts_us":1792970703118632,"ts_ns":1792970703118632969}",
          "kafka_ts": "2026-10-25T23:25:03.296Z",
          "ingested_at": "2026-10-25T23:25:03.317Z"
        }
      ]
    },
    {
      "file_path": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12959.parquet",
      "byte_count": 4780,
      "sha256": "7d97f145e9dd0fe47e7db8a69498012ab1de3d702435c9d6fa78475bb7327d53",
      "magic_start": "PAR1",
      "magic_end": "PAR1",
      "decoded_record_count": 1,
      "matching_records": [
        {
          "topic": "cdc.public.refunds",
          "partition_id": 0,
          "kafka_offset": 9,
          "payload": "{"before":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":20000,"status":"success"},"after":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":30000,"status":"success"},"source":{"version":"3.2.4.Final","connector":"postgresql","name":"cdc","ts_ms":1792970813039,"snapshot":"false","db":"app","sequence":"[\"26845320\",\"26845376\"]","ts_us":1792970813039276,"ts_ns":1792970813039276000,"schema":"public","table":"refunds","txId":771,"lsn":26845376,"xmin":null},"transaction":null,"op":"u","ts_ms":1792970813389,"ts_us":1792970813389354,"ts_ns":1792970813389354464}",
          "kafka_ts": "2026-10-25T23:26:53.733Z",
          "ingested_at": "2026-10-25T23:26:53.755Z"
        }
      ]
    },
    {
      "file_path": "s3://warehouse/bronze/cdc_events/data/00000-0-f4a2f1e7-1fa9-4e11-9ef3-4baa43807b71-12961.parquet",
      "byte_count": 4395,
      "sha256": "4f2c902194d58944eaaf0d5b926cc79d0e77a086c040a4dbd41aa76f651130d4",
      "magic_start": "PAR1",
      "magic_end": "PAR1",
      "decoded_record_count": 1,
      "matching_records": [
        {
          "topic": "cdc.public.refunds",
          "partition_id": 0,
          "kafka_offset": 10,
          "payload": "{"before":{"refund_id":"s01-junekim-01-r1","payment_id":"s01-junekim-01-p1","amount":30000,"status":"success"},"after":null,"source":{"version":"3.2.4.Final","connector":"postgresql","name":"cdc","ts_ms":1792970832561,"snapshot":"false","db":"app","sequence":"[\"26845608\",\"26845872\"]","ts_us":1792970832561839,"ts_ns":1792970832561839000,"schema":"public","table":"refunds","txId":772,"lsn":26845872,"xmin":null},"transaction":null,"op":"d","ts_ms":1792970832712,"ts_us":1792970832712512,"ts_ns":1792970832712512501}",
          "kafka_ts": "2026-10-25T23:27:12.823Z",
          "ingested_at": "2026-10-25T23:27:12.843Z"
        }
      ]
    }
  ],
  "passed": true,
  "sql_file_records_match": true,
  "matching_record_count": 3,
  "three_event_history_complete": true,
  "observed_at": "2026-10-25T23:39:03.358660+00:00",
  "evidence_path": "artifacts/runs/session02/s01-junekim-01/parquet.json"
} 실행을 통해 MinIO S3에서 물리 바이트를 직접 읽어 Parquet Magic Header() 및 디코딩된 레코드를 파싱하였으며, 3개 파일에 걸쳐 matching_record_count=3 확인 완료.
- 파일 안에서 찾은 payload와 수집 위치:
  각 Parquet 파일 내에 물리 컬럼 , , , , , 가 온전하게 보관되어 있음을 확인.
- 위 결과의 근거: 개인 실행
- 파일 하나의 행 수와 이번 환불의 전체 이력 수가 다를 수 있는 이유:
  Flink 스트리밍은 설정된 Checkpoint 주기마다 버퍼링된 레코드들을 묶어 Parquet 파일로 플러시하고 스냅샷을 커밋하므로, 한 체크포인트 주기 내에 여러 이벤트가 묶이거나 여러 체크포인트에 걸쳐 이벤트들이 분할 저장되기 때문입니다.
- 현재 Iceberg 테이블 조회와 폴더의 파일 직접 읽기를 구분해야 하는 이유:
  MinIO S3 폴더에는 과거 스냅샷에서 생성되었으나 컴팩션(병합)되거나 롤백된 구버전 데이터 파일이 물리적으로 잔류할 수 있습니다. 반면 Iceberg 테이블 조회는 최신 메타데이터 JSON과 Manifest List가 가리키는 활성 스냅샷 파일들만 논리적으로 읽기 때문에 항상 일치하지 않습니다.
- 실제 파일 내용 캡처 또는 오류·질문:
  물리 파켓 파일 sha256 및 magic_start='PAR1', magic_end='PAR1' 검증 완료 ().

## 5. 작은 Python 파서와 SQL

- fixture 실행·확인 결과:
  {"origin": "synthetic_fixture", "sample_id": "r", "kind": "change", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 0}, "op": "r", "pk": {"refund_id": "fixture-r1"}, "pk_source": "after", "before": null, "after": {"refund_id": "fixture-r1", "amount": 20000}, "row": {"refund_id": "fixture-r1", "amount": 20000}, "source_lsn": 100}
{"origin": "synthetic_fixture", "sample_id": "c", "kind": "change", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 1}, "op": "c", "pk": {"refund_id": "fixture-r1"}, "pk_source": "after", "before": null, "after": {"refund_id": "fixture-r1", "amount": 20000}, "row": {"refund_id": "fixture-r1", "amount": 20000}, "source_lsn": 101}
{"origin": "synthetic_fixture", "sample_id": "u", "kind": "change", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 2}, "op": "u", "pk": {"refund_id": "fixture-r1"}, "pk_source": "after", "before": {"refund_id": "fixture-r1", "amount": 20000}, "after": {"refund_id": "fixture-r1", "amount": 30000}, "row": {"refund_id": "fixture-r1", "amount": 30000}, "source_lsn": 102}
{"origin": "synthetic_fixture", "sample_id": "d", "kind": "change", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 3}, "op": "d", "pk": {"refund_id": "fixture-r1"}, "pk_source": "before", "before": {"refund_id": "fixture-r1", "amount": 30000}, "after": null, "row": {"refund_id": "fixture-r1", "amount": 30000}, "source_lsn": 103}
{"origin": "synthetic_fixture", "sample_id": "wire-tombstone", "kind": "tombstone", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 4}, "op": null, "pk": {"refund_id": "fixture-r1"}, "value_kind": "wire_null"}
{"origin": "synthetic_fixture", "sample_id": "malformed-json", "kind": "invalid", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 5}, "error_code": "invalid_value_json"}
{"origin": "synthetic_fixture", "sample_id": "json-null", "kind": "invalid", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 6}, "error_code": "invalid_json_null"}
{"origin": "synthetic_fixture", "sample_id": "invalid-key-type", "kind": "invalid", "metadata": {"topic": "fixture.refunds", "partition_id": 0, "kafka_offset": 7}, "error_code": "invalid_key_type"} 실행 결과 8개 테스트 케이스(r, c, u, d, wire-tombstone, malformed-json, json-null, invalid-key-type) 모두 100% 정상 파싱 및 예외 분류 완료.
- c/u/d/r에서 사용하는 before/after와 SQL [D]의 관계:
  r과 c는 after 객체에서 값을 읽고, u는 before/after를 모두 포함하며, d는 after가 null이므로 before 객체에서 값을 읽습니다. SQL [D]의 COALESCE(after_id, before_id) 및 CASE WHEN op='d' THEN before_amount ELSE after_amount END 로직이 파서의 row 추출 로직과 정확히 1:1로 대응됩니다.
- tombstone·손상된 JSON·모르는 op를 구분하는 이유:
  tombstone(value=null)을 일반 JSON처럼 파싱하면 TypeError/NullPointerException이 발생하고, 손상된 JSON이나 미지원 op가 유입될 때 스트리밍 전체가 중단(크래시)되는 것을 방지하기 위해 Dead Letter Queue(DLQ) 격리 및 사전 필터링이 필수적이기 때문입니다.
- fixture 결과와 개인 Kafka·Bronze 실행 결과가 각각 확인하는 범위:
  파서 fixture는 단일 이벤트 문자열에 대한 로컬 파싱 단위 테스트(Unit Test) 범위이며, 개인 실행은 원천 WAL 복제 -> Kafka 브로커 전달 -> Flink 체크포인트 -> MinIO S3 I/O -> Iceberg Catalog 커밋에 이르는 End-to-End 분산 파이프라인 정합성을 확인합니다.

## 6. 진단과 남은 질문

선택한 상황: **상황 3: Bronze에서 c와 u만 조회되고 DELETE가 조회되지 않음**
- 확인할 구간·설정·조회 조건:
  가장 먼저 Spark SQL의 WHERE 절에 after.refund_id만 단독으로 걸려 있는지 확인하고, before.refund_id 조건이 누락되었는지 점검합니다.
- 필요한 증거:
  1) Spark SQL 쿼리문: OR get_json_object(payload, '$.'before.refund_id') = '...' 포함 여부
  2) Kafka 토픽 증거: kafka-console-consumer로 해당 키의 토픽 마지막 오프셋에 op='d' 메시지가 실제로 도착했는지 확인
- 이번 결과에서 아직 단정할 수 없는 것:
  쿼리 조건이 올바름에도 DELETE가 안 보인다면 Flink의 Checkpoint가 아직 완료되지 않아 Iceberg Sink가 새 스냅샷을 Catalog에 커밋하기 전일 수 있으므로 즉각적인 수집 누락으로 단정할 수 없습니다.

### 📚 Apache Iceberg 사전 학습 메모

1. **어떤 일을 하는 기술로 이해했나요?**:
   Apache Iceberg는 대규모 데이터 레이크(S3, MinIO 등)에 저장된 파일(Parquet, ORC) 위에 RDBMS 수준의 **ACID 트랜잭션, 타임 트래블(Time-Travel/스냅샷 격리), 스키마 진화(Schema Evolution), 그리고 고성능 파티션 프루닝(Hidden Partitioning)**을 제공하는 고성능 오픈소스 오픈 테이블 포맷(Open Table Format)입니다. 기존 Hive 방식의 '폴더' 기반 메타데이터 관리 한계를 극복하고, '트리 구조의 계층형 메타데이터(Metadata JSON -> Manifest List -> Manifest Files)'를 통해 파일 단위로 정합성을 추적하여 동시성 제어와 쿼리 성능을 극대화합니다.
2. **궁금하거나 이해되지 않는 점**:
   CDC 환경처럼 실시간 스트리밍으로 소량의 데이터가 연속 적재될 때 발생하는 스몰 파일(Small Files) 문제에 대해, Flink가 백그라운드에서 실시간 스트리밍 쓰기를 지속하는 동시에 Iceberg의 유지관리 작업(Data File Compaction / Rewrite Manifests)이 수행될 때 OCC(낙관적 동시성 제어) 충돌이 어떻게 회피되는지 실무적 튜닝 기법이 궁금합니다.
3. **참고 자료 링크**:
   - [Apache Iceberg 공식 문서 (v1.10.0)](https://iceberg.apache.org/docs/1.10.0/)
   - [Debezium PostgreSQL Connector Documentation](https://debezium.io/documentation/reference/3.2/connectors/postgresql.html)

---

## 제출 전 확인

- [x] 개인 환경에서 공통 예제를 실행하고 결과 또는 막힌 지점을 정리했습니다.
- [x] student-worksheet.sql의 SQL·예측·진단 답을 정리했습니다.
- [x] 관찰하지 못한 값과 참고한 자료의 출처를 구분했습니다.
- [x] 캡처 이미지를 문서와 함께 첨부해 다른 사람이 열 수 있는지 확인했습니다.
