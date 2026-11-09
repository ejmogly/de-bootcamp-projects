[PostgreSQL (Source WAL)] 
       │ (논리적 복제: REPLICA IDENTITY FULL)
       ▼
 [Debezium Connect] ──▶ [Apache Kafka] (Streaming Buffer)
                              │
                              ▼
                     [Apache Flink] (실시간 스트리밍 적재)
                              │
                              ▼
                 [Apache Iceberg (Bronze)] (MinIO S3 + JDBC Catalog)
                              │
                     [Spark SQL] ──▶ [Iceberg Silver] (Upsert/Merge)
                              │
                    [dbt + Airflow] ──▶ [Iceberg Gold] (분석 마트)
