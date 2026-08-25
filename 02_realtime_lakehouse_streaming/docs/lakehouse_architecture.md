# 🏛️ Real-time Lakehouse Architecture Deep Dive

## 1. Why Open Table Format (Iceberg / Paimon)?
- **Traditional Hive Metastore / Raw Parquet on S3 Issues**:
  - Small File Problem: Real-time streaming generates millions of tiny files, causing S3 ListObjects bottleneck.
  - Lack of ACID: Concurrent writes cause dirty reads or incomplete queries.
  - No Upsert/Delete: Regulatory GDPR/CCPA compliance requires heavy rewrites.
- **Iceberg / Paimon Solutions**:
  - Hidden Partitioning: Users do not need to specify partition filters manually.
  - Snapshot Isolation & Time Travel: Historical audit and rollback capability.
  - Automatic Compaction: Asynchronous rewriting of small files into optimized Parquet.
