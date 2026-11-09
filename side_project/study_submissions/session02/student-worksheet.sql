-- 2회차 — 원천 설정부터 Kafka·Bronze·파일까지 같은 변경 따라가기
-- 수업 중에는 강사 화면을 보며 SQL·예측을 작성합니다.
-- 수업 후에는 개인 환경에서 공통 예제를 실행하고 실제 결과와 대조합니다.
-- 이 파일을 통째로 실행하지 마세요. [A][D]는 Spark SQL, [B]는 PostgreSQL입니다.
-- [C][E]는 읽기·예측 질문입니다. Python 파서의 실행 방법은 학생 가이드를 봅니다.
-- 이름 / 학생 ID: 김준 (junekim) / de_bootcamp_7th
-- 개인 실행 CASE_ID / 환불 ID: s01-junekim-01 / s01-junekim-01-r1

-- [A] Spark SQL — Bronze의 c/u/d 세 이력과 파일
-- 1. 같은 환불을 20000 입력 → 30000 정정 → 삭제하면 어떻게 될까요?
-- 예상 원천 현재 행 수: 0행 (DELETE로 인해 원천 DB에서는 물리 삭제됨)
-- 예상 Bronze 이력 수와 op 순서: 3행, op 순서는 'c'(생성) -> 'u'(수정) -> 'd'(삭제)
-- DELETE 이벤트에서 refund_id를 찾을 곳: before.refund_id (DELETE 시 after는 null이므로 before 객체 또는 Kafka Key에서 확인)

-- 아래 쿼리는 after.refund_id만 조건으로 사용합니다.
-- TODO: DELETE 이력도 포함하도록 before.refund_id 조건을 추가하세요.
SELECT get_json_object(payload, '$.op') AS op,
       get_json_object(payload, '$.before.amount') AS before_amount,
       get_json_object(payload, '$.after.amount') AS after_amount,
       get_json_object(payload, '$.source.lsn') AS source_lsn,
       topic, partition_id, kafka_offset, _file AS parquet_file
FROM lake.bronze.cdc_events
WHERE topic = 'cdc.public.refunds'
  AND (get_json_object(payload, '$.after.refund_id') = 's01-junekim-01-r1'
       OR get_json_object(payload, '$.before.refund_id') = 's01-junekim-01-r1')
ORDER BY partition_id, kafka_offset;

-- 2. Iceberg 현재 테이블이 참조하는 데이터 파일 목록입니다.
-- 전체 테이블의 파일 목록이며 위 환불 ID만의 파일 목록은 아닙니다.
SELECT file_path, file_format, record_count, file_size_in_bytes
FROM lake.bronze.cdc_events.files
WHERE content = 0
ORDER BY file_path
LIMIT 10;

-- 위 결과는 데이터 파일 일부입니다. [A]-1의 parquet_file과 file_path를 연결합니다.
-- 해당 파일이 첫 10개에 없다면 file_path 조건으로 직접 찾습니다.

-- TODO: 강사가 실제 Parquet 내용을 읽는 화면에서 아래를 찾아보세요.
-- 파일에 저장된 payload·topic·partition_id·kafka_offset: Parquet 스키마의 물리 컬럼으로 직접 저장됨
-- Kafka value 원문에 있던 op와 금액은 파일의 어느 컬럼에 들어 있나요? payload 컬럼 내부의 JSON 문자열 속성에 보관됨
-- 세 이벤트가 반드시 한 파일에 들어가거나 DML 하나마다 파일 하나가 생길까요? 아니다. Flink Checkpoint 주기 내에 커밋된 이벤트들이 파일 단위로 묶이므로 DML과 파일은 1:1이 아님
-- MinIO 폴더에 있는 모든 파일과 Iceberg 현재 테이블이 참조하는 파일은 같을까요? 다르다. 과거 만료된 스냅샷 파일이나 롤백된 파일이 MinIO에 잔류할 수 있으며, 현재 테이블은 현재 manifest가 가리키는 파일만 참조함

-- [B] PostgreSQL — 수집 조건을 읽기 전용으로 확인하기
-- 아래 SELECT/SHOW는 원천 PostgreSQL에만 실행합니다.
-- publication·slot을 삭제하거나 replica identity를 변경하는 실습이 아닙니다.
SHOW wal_level;

SELECT pubname, puballtables, pubinsert, pubupdate, pubdelete
FROM pg_publication
WHERE pubname = 'cdc_lab';

SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'cdc_lab'
ORDER BY schemaname, tablename;

SELECT n.nspname AS schema_name,
       c.relname AS table_name,
       c.relreplident AS replica_identity
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public'
  AND c.relname IN ('orders', 'payments', 'refunds')
ORDER BY c.relname;

SELECT slot_name, plugin, slot_type, database, active,
       restart_lsn, confirmed_flush_lsn
FROM pg_replication_slots
WHERE slot_name = 'cdc_lab';

-- TODO: 아래 역할을 각각 한 줄로 설명하세요.
-- publication — 어떤 테이블/변경을 발행하는가: cdc_lab은 orders, payments, refunds 테이블의 INSERT, UPDATE, DELETE, TRUNCATE 전체 변경 이벤트를 발행함
-- replication slot — 어느 소비자의 진행 위치·WAL 보존과 관련되는가: Debezium 커넥터(pgoutput)의 소비 진행 위치(confirmed_flush_lsn)를 추적하고 소비 전 WAL 삭제를 방지함
-- REPLICA IDENTITY FULL — before.amount 관찰과 어떤 관계가 있는가: 테이블 변경 전 전체 컬럼 값을 WAL에 기록하여 UPDATE/DELETE 시 before.amount를 완전하게 캡처할 수 있게 함
-- slot active=true이면 Bronze 반영까지 완료됐다고 할 수 있나요? 아니다. Debezium 연결 활성 상태만 뜻하며 Kafka 전달 및 Flink/Iceberg 커밋 반영은 별개임

-- [C] Debezium·Kafka — 설정과 메시지를 연결하기
-- ./labs/session02/lab.sh connector-sanitized 출력에서 비밀번호를 제외한 설정을 읽습니다.
-- snapshot.mode='initial'과 새 DML의 op=c/u/d는 어떻게 다른가요? initial은 기동 시 기존 데이터의 전체 상태를 읽어 op='r'로 내보내고, 이후 변경은 WAL을 통해 op='c/u/d'로 실시간 발행됨
-- 이미 수집 중인 커넥터를 재기동하면 매번 처음부터 r을 발행하나요? 아니다. connect-offsets 토픽과 replication slot에 커밋된 LSN이 저장되어 있으므로 마지막 오프셋부터 증분 재개됨
-- table.include.list와 PostgreSQL publication 중 한쪽에만 테이블이 있으면 충분할까요? 불충분하다. publication에 등록되어 WAL로 발행되고, 동시에 Debezium include list에도 포함되어야 Kafka로 수집됨
-- topic.prefix='cdc'일 때 refunds 토픽 이름: cdc.public.refunds
-- 같은 환불의 Kafka key와 value 안의 refund_id는 각각 어디에 있나요? Key는 루트의 {"refund_id": "..."}에 있고, Value는 after.refund_id (c, u) 또는 before.refund_id (d)에 있음
-- op=d의 value와 tombstone의 value는 어떻게 다른가요? op='d'는 before 객체를 담은 유효한 Debezium JSON 문자열이고, tombstone은 Kafka Compaction용으로 발행되는 null 바이트(value=null) 자체임
-- 같은 이벤트를 Kafka와 Bronze에서 대조할 세 항목: topic, partition_id (partition), kafka_offset (offset)
-- PK / source.lsn / Kafka 위치 중 단독 전역 고유 이벤트 ID로 쓰면 안 되는 것과 이유: PK는 c/u/d 변경마다 동일하게 반복되고, LSN은 단일 트랜잭션 내 여러 행이 공유할 수 있으므로 단독 고유 ID로 사용 불가함

-- [D] Spark SQL — 작은 파서의 해석을 SQL로 다시 표현하기
WITH decoded AS (
  SELECT get_json_object(payload, '$.op') AS op,
         get_json_object(payload, '$.before.refund_id') AS before_id,
         get_json_object(payload, '$.after.refund_id') AS after_id,
         CAST(get_json_object(payload, '$.before.amount') AS BIGINT) AS before_amount,
         CAST(get_json_object(payload, '$.after.amount') AS BIGINT) AS after_amount,
         topic, partition_id, kafka_offset
  FROM lake.bronze.cdc_events
  WHERE topic = 'cdc.public.refunds'
)
SELECT op,
       COALESCE(after_id, before_id) AS refund_id,
       CASE WHEN op = 'd' THEN before_amount ELSE after_amount END AS event_amount,
       topic, partition_id, kafka_offset
FROM decoded
WHERE COALESCE(after_id, before_id) = 's01-junekim-01-r1'
ORDER BY partition_id, kafka_offset;

-- TODO: Python 파서와 위 SQL을 읽고 답하세요.
-- 실제 20000→30000→DELETE 시나리오에서 c / u / d의 event_amount 기대값: c: 20000 (after), u: 30000 (after), d: 30000 (before)
-- DELETE에서 after만 읽으면 무엇을 잃나요? after는 null이므로 refund_id 식별자와 직전 환불 금액(30000) 등 모든 비즈니스 맥락을 잃어버림
-- r 이벤트를 새 INSERT c와 구분해야 하는 이유: r은 수집 전 과거 원천의 기준 시점 스냅샷 데이터이고 c는 가동 후 실시간 신규 이벤트이므로 시간축 분석 및 비즈니스 트리거 분리가 필요함
-- tombstone을 일반 JSON 객체처럼 읽으면 어떻게 될까요? value가 null이므로 json.loads 또는 파싱 함수에서 TypeError/NullPointerException 에러 발생
-- 손상된 JSON이나 모르는 op를 만났을 때 무엇을 알려줘야 할까요? 파이프라인 중단 없이 에러 코드, 원본 오프셋/파티션 정보를 DLQ나 모니터링 로그에 기록하여 관리자에게 통보해야 함
-- 파서 fixture를 통과하면 실제 DB→Bronze 수집도 검증됐다고 할 수 있나요? 아니다. 단일 JSON 해석 로직에 대한 단위 테스트일 뿐이며 네트워크, 커넥터, 체크포인트, Iceberg 커밋 등 전체 파이프라인 정합성과는 무관함

-- [E] 진단 — 가장 먼저 볼 구간과 필요한 증거를 적습니다.
-- 상황 1: 원천 UPDATE는 보이는데 Kafka에 해당 키의 u가 없습니다.
-- 먼저 볼 구간 / 필요한 증거: Debezium Connector 및 pg_replication_slots / slot active 상태, restart_lsn 진행 여부, Debezium task 에러 로그
-- 상황 2: Kafka에는 u가 있지만 Bronze에는 없습니다.
-- 먼저 볼 구간 / 필요한 증거: Flink Job 상태 및 Iceberg Checkpoint / Flink RUNNING 상태, Checkpoint completed 카운트 증가 여부, MinIO I/O 및 Catalog DB 커밋 로그
-- 상황 3: Bronze에서 c와 u만 조회됩니다. DELETE가 누락됐을까요?
-- 먼저 볼 조회 조건 / 비교할 원천·Kafka 증거: WHERE 절에 before.refund_id 조건 포함 여부 / Kafka 토픽에 op='d' 메시지 오프셋 존재 여부 대조
-- 상황 4: Parquet 한 파일에는 c만 있습니다. u/d 수집 실패일까요?
-- 먼저 볼 파일 목록·테이블 조회 / 판단을 보류할 이유: lake.bronze.cdc_events.files 메타데이터 / Flink 체크포인트 주기에 따라 이후 이벤트는 다른 Parquet 파일에 커밋되므로 정상 동작임
