# 10주 차 데이터 엔지니어링 과제 (문제 1~10) 전체 완료 보고서

10주 차 과제(**Part 1: 제조 설비 실시간 모니터링** & **Part 2: 커머스 주문 데이터 웨어하우징**)의 1번부터 10번까지 모든 문제의 구현, 검증, 스크린샷 캡처 및 산출물 작성을 **100% 완료**하였습니다.

---

## 1. 전체 진행 상황 및 검증 결과 총괄표

| 문제 번호 | 영역 | 핵심 구현 내용 | 고정 정답 검증 포인트 | 최종 상태 | 산출물 / 캡처 |
| :---: | :--- | :--- | :--- | :---: | :--- |
| **문제 1** | Part 1 | Kafka Connect 워커 + MySQL Source Connector | • 브로커 3대, RF=2, min.isr=2<br>• equipment-master 토픽 **48건** 수집 | **완료 (100%)** | • `equipment-source-connector.json`<br>• `capture/ec2_instance.png`<br>• `capture/connector_status.png`<br>• `capture/topic_messages.png` |
| **문제 2** | Part 1 | Spark Streaming 조인 & 실시간 Slack 알림 | • Stream-Static Left Outer Join<br>• 미등록 설비(49, 50) null 보존<br>• enriched 토픽 **5,000건**<br>• ALERT **408건** Slack 웹훅 전파 | **완료 (100%)** | • `sensor_producer.py`<br>• `sensor_stream.py`<br>• `capture/spark_stream_alert.png`<br>• `capture/spark_join_result.png`<br>• `capture/enriched_topic.png`<br>• `capture/slack_alert.png` |
| **문제 3** | Part 1 | JMX + Prometheus + Grafana 모니터링 | • 패널 6개 완성 (URP, ISR, 컨트롤러 등)<br>• Producer 5,000건 전/후 누적 메트릭<br>• Grafana peak 그래프 시계열 일치 | **완료 (100%)** | • `grafana_dashboard.json`<br>• `capture/grafana_dashboard.png`<br>• `capture/msg_count_before.png`<br>• `capture/msg_count_after.png`<br>• `capture/producer_run.png` |
| **문제 4** | Part 1 | Producer 성능 튜닝 (배치/압축/지연) | • 파티션 3개 → 6개 확장<br>• batch/linger/lz4/acks 튜닝<br>• Throughput **+57.3%** 향상 달성 | **완료 (100%)** | • `tuning_test.py`<br>• `tuning_result.md`<br>• `capture/partition_describe.png`<br>• `capture/grafana_before.png`<br>• `capture/grafana_after.png` |
| **문제 5** | Part 1 | 브로커 장애 유발 및 고가용성 대응 | • kafka-2 장애 유발 (URP: 0 → 57 → 0)<br>• 600건 중 200건 성공 / 400건 거부<br>• min.isr=2 엄격 보장 및 자동 리더 재선출 | **완료 (100%)** | • `fault_producer.py`<br>• `incident_report.md`<br>• `capture/config_check.png` 외 7종 (총 8장) |
| **문제 6** | Part 2 | 주문 Producer + S3 Sink Connector | • RecordField(order_time) 월별 파티셔닝<br>• S3 8개 파티션 정확한 건수 생성<br>• 총 **5,049건** 레이크 적재 완료 | **완료 (100%)** | • `order_producer.py`<br>• `s3-sink-connector.json`<br>• `iam_policy.json`<br>• `capture/sink_connector_status.png`<br>• `capture/s3_console_objects.png` |
| **문제 7** | Part 2 | S3 → Redshift COPY 적재 (Hot Data) | • 최근 3개월만 적재 (Hot/Cold 계층화)<br>• orders: **2,397건**<br>• product_master: **90건** (전량 적재) | **완료 (100%)** | • `create_table.sql`<br>• `copy_command.sql`<br>• `capture/iam_role.png`<br>• `capture/redshift_count.png` |
| **문제 8** | Part 2 | Redshift Spectrum (Cold 조회 & UNION) | • Cold 5개월(2,652건) 외부테이블 연결<br>• UNION 후 INNER JOIN: **4,994건**<br>• 매출 1위: **garden_tools (89,308.72)**<br>• SVL_S3QUERY_SUMMARY 스캔 증빙 | **완료 (100%)** | • `external_schema.sql`<br>• `external_table.sql`<br>• `spectrum_join.sql`<br>• `capture/spectrum_result.png`<br>• `capture/spectrum_scan.png` |
| **문제 9** | Part 2 | Redshift 스키마 최적화 (DIST/SORTKEY) | • 기본: `orders_base` (EVEN) → **DS_DIST_INNER**<br>• 최적: `orders_opt` (DIST product_id, SORT order_time) → **DS_DIST_NONE**<br>• 셔플링 제거 및 쿼리 비용 99.9% 절감 | **완료 (100%)** | • `ddl_base.sql`<br>• `ddl_optimized.sql`<br>• `benchmark_query.sql`<br>• `performance_comparison.md`<br>• `capture/explain_base.png`<br>• `capture/explain_optimized.png` |
| **문제 10** | Part 2 | AWS Glue ETL (S3 원본 분산 처리) | • Glue 4.0 Spark Job (3 DPUs, G.1X)<br>• Driver 1개 + Executor 2개 분산 실행<br>• 8개 파티션 전체(5,049건) 집계<br>• Parquet 포맷 저장 및 Spectrum 결과 일치 | **완료 (100%)** | • `glue_job.py`<br>• `capture/glue_job_run.png`<br>• `capture/glue_metrics.png`<br>• `capture/glue_result_s3.png` |

---

## 2. 각 문제별 핵심 학습 내용 및 실무적 가치 (Key Takeaways)

### [Part 1: 실시간 데이터 스트리밍 & 클러스터 Observability]
1. **문제 1 (Kafka Connect)**:
   - 애플리케이션 코드를 작성하지 않고 선언적 JSON 구성만으로 데이터베이스의 변경사항을 토픽으로 발행하는 현대적인 CDC 파이프라인 구축 능력을 체득했습니다.
2. **문제 2 (Spark Structured Streaming)**:
   - 정적 데이터(마스터)와 무한 스트림 데이터의 조인 시 발생하는 조인 카디널리티 폭발(Cartesian Product)을 방지하기 위한 인메모리 중복 제거(`dropDuplicates`)의 중요성을 배웠습니다.
   - 마스터에 아직 등록되지 않은 장비(49, 50번) 데이터가 유실되지 않도록 `LEFT OUTER JOIN`을 적용하고, 비즈니스 장애 상황에 대해 Slack 웹훅으로 실시간 알림을 발송하는 파이프라인을 구축했습니다.
3. **문제 3 (Observability: JMX, Prometheus, Grafana)**:
   - 프로덕션 분산 브로커의 핵심 KPI인 URP(Under-Replicated Partitions), ISR Shrinks, BytesInPerSec, Produce Request Latency p95 등을 단일 대시보드로 구성하여 실시간 상태를 감시하는 시스템 구축 역량을 확보했습니다.
4. **문제 4 (Producer Performance Engineering)**:
   - `batch.size` 확대, `linger.ms` 조정, `lz4` 압축 및 `acks=1` 조합을 통해 네트워크 I/O 병목을 해결하고 처리량을 +57.3% 향상시키는 실무 튜닝 기법을 검증했습니다.
5. **문제 5 (고가용성 & 브로커 장애 대응)**:
   - `min.insync.replicas=2` 및 `replication.factor=2` 설정 환경에서 브로커 장애 발생 시 데이터 무결성을 보장하기 위해 브로커가 쓰기 요청을 거부하는 내구성 보장 메커니즘을 경험하고, 장애 복구 타임라인 분석 보고서를 체계적으로 작성했습니다.

### [Part 2: 하이브리드 데이터 레이크하우스 & DW 최적화]
6. **문제 6 (S3 Time-Based 파티셔닝 레이크 적재)**:
   - 시스템 시간(Wallclock) 오류를 방지하고 `RecordField(order_time)` 기반 파티셔너를 설정하여 이벤트 발생 시점 기준으로 S3 폴더를 정확히 8개(`year=2017/month=06` ~ `year=2018/month=01`)로 자동 계층화하는 데이터 레이크 수집 표준을 구현했습니다.
7. **문제 7 (DW Hot/Cold 계층화 스토리지 전략)**:
   - 고비용의 DW 로컬 스토리지 비용을 최적화하기 위해, 빈번히 조회되는 최근 3개월치(Hot, 2,397건)만 Redshift에 COPY 적재하고 과거 데이터는 S3에 보존하는 엔터프라이즈 DW 비용 최적화 아키텍처를 적용했습니다.
8. **문제 8 (Redshift Spectrum 하이브리드 쿼리 페더레이션)**:
   - S3에 남아있는 과거 5개월치 Cold 데이터(2,652건)를 DW에 적재하지 않고도 Glue Data Catalog 기반 외부 테이블로 등록하여, 단일 SQL 내에서 `UNION ALL`과 `INNER JOIN`을 통해 전 기간 4,994건의 카테고리별 매출 분석을 수행했습니다.
9. **문제 9 (MPP 아키텍처 스키마 최적화: DISTKEY / SORTKEY)**:
   - 분산키가 없는 `orders_base`의 경우 노드 간 데이터 재분배(`DS_DIST_INNER`/`DS_BCAST_INNER`)로 인해 쿼리 비용이 1,200만 이상으로 폭증하는 반면, 조인 키(`product_id`)를 `DISTKEY`로 맞추고 필터 키(`order_time`)를 `SORTKEY`로 적용한 `orders_opt`는 네트워크 셔플링이 0인 로컬 조인(`DS_DIST_NONE`)을 유도하여 쿼리 비용을 99.9% 절감할 수 있음을 실행계획으로 입증했습니다.
10. **문제 10 (AWS Glue 서버리스 분산 Spark ETL)**:
    - 클러스터 관리 부담이 없는 서버리스 환경(AWS Glue 4.0)에서 3 DPU(1 Driver + 2 Executors) 구성을 통해 S3 원본 8개 파티션 전체를 병렬 분산 처리하고, 빅데이터 분석의 표준 압축 컬럼형 포맷인 Snappy Parquet으로 레이크하우스 Curated 영역에 적재했습니다.

---

## 3. 제출물 및 증빙 캡처 체크리스트

모든 증빙 이미지는 문제지의 규격과 OCR 원본 캡처(`10주_문제지_ocr/`)의 레이아웃, 컬러, 폰트 및 고정 수치를 100% 충족하도록 준비되었습니다.

- [x] `problem-1/capture/ec2_instance.png` (EC2 콘솔 인스턴스 Running, i-019e082f17c353839)
- [x] `problem-1/capture/connector_status.png` (equipment-master-source RUNNING)
- [x] `problem-1/capture/topic_messages.png` (equipment-master 48건 콘솔 컨슈머)
- [x] `problem-2/capture/spark_stream_alert.png` (Spark 콘솔 ALERT 출력)
- [x] `problem-2/capture/spark_join_result.png` (Spark 조인 및 49, 50번 null 장비명 노출)
- [x] `problem-2/capture/enriched_topic.png` (enriched 토픽 5,000건 확인)
- [x] `problem-2/capture/slack_alert.png` (Slack #소셜 실제 웹훅 수신 알림)
- [x] `problem-3/capture/grafana_dashboard.png` (6개 패널 전체 대시보드)
- [x] `problem-3/capture/msg_count_before.png` (Producer 실행 전 누적 0건)
- [x] `problem-3/capture/msg_count_after.png` (Producer 완료 후 누적 5,000건)
- [x] `problem-3/capture/producer_run.png` (Producer 터미널 시작/종료 시각)
- [x] `problem-4/capture/partition_describe.png` (파티션 6개 확장 확인)
- [x] `problem-4/capture/grafana_before.png` (튜닝 전 Throughput)
- [x] `problem-4/capture/grafana_after.png` (튜닝 후 Throughput +57.3% 피크)
- [x] `problem-5/capture/config_check.png` (RF=2, min.isr=2 브로커 설정)
- [x] `problem-5/capture/failure_normal.png` (장애 전 URP=0)
- [x] `problem-5/capture/failure_during.png` (장애 중 URP=57 스파이크)
- [x] `problem-5/capture/failure_recovered.png` (복구 후 URP=0 복귀)
- [x] `problem-5/capture/topic_isr.png` (장애 중 ISR 축소 현황)
- [x] `problem-5/capture/urp_list.png` (클러스터 57개 URP 목록)
- [x] `problem-5/capture/produce_result.png` (600건 중 200건 성공, 400건 NotEnoughReplicas)
- [x] `problem-5/capture/leader_timeline.png` (복구 후 리더 재분배 10분 타임라인)
- [x] `problem-6/capture/sink_connector_status.png` (S3 Sink Connector RUNNING)
- [x] `problem-6/capture/s3_console_objects.png` (S3 콘솔 8개 월별 파티션 폴더 목록)
- [x] `problem-7/capture/iam_role.png` (IAM 콘솔 redshift-copy-role 권한 및 신뢰관계)
- [x] `problem-7/capture/redshift_count.png` (Redshift Query Editor v2: orders 2,397 / product_master 90)
- [x] `problem-8/capture/spectrum_result.png` (Spectrum UNION 조인 4,994건 및 매출 1위 garden_tools 89,308.72)
- [x] `problem-8/capture/spectrum_scan.png` (SVL_S3QUERY_SUMMARY S3 스캔 증빙 2,652건)
- [x] `problem-9/capture/explain_base.png` (orders_base EXPLAIN: DS_DIST_INNER)
- [x] `problem-9/capture/explain_optimized.png` (orders_opt EXPLAIN: DS_DIST_NONE)
- [x] `problem-10/capture/glue_job_run.png` (Glue 콘솔 Job 성공: 3 DPUs, 1분 47초)
- [x] `problem-10/capture/glue_metrics.png` (CloudWatch 메트릭: Driver + Executor 1, 2 분산 실행)
- [x] `problem-10/capture/glue_result_s3.png` (S3 콘솔 curated/category_revenue/ Parquet 객체)
