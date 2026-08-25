import json
import gzip
import io
import urllib.parse
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Event-driven S3 trigger:
    Raw JSON.gz Ingestion -> Gzip Decompress -> Parse & Sanitize -> Parquet Convert -> Target S3
    """
    for record in event['Records']:
        src_bucket = record['s3']['bucket']['name']
        src_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        # 1. Download & Decompress
        response = s3_client.get_object(Bucket=src_bucket, Key=src_key)
        gz_body = response['Body'].read()
        decompressed_data = gzip.decompress(gz_body).decode('utf-8')
        
        # 2. Parse Lines
        records = []
        for line in decompressed_data.strip().split('\n'):
            if line:
                records.append(json.loads(line))
        
        if not records:
            continue
            
        # 3. Convert to PyArrow Table
        table = pa.Table.from_pylist(records)
        
        # 4. Write to Parquet in-memory
        pq_buffer = io.BytesIO()
        pq.write_table(table, pq_buffer, compression='SNAPPY')
        pq_buffer.seek(0)
        
        # 5. Partitioned S3 Key
        now = datetime.utcnow()
        dst_bucket = 'processed-analytics-lake'
        dst_key = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/processed_{src_key.split('/')[-1].replace('.json.gz', '.parquet')}"
        
        s3_client.put_object(
            Bucket=dst_bucket,
            Key=dst_key,
            Body=pq_buffer.getvalue()
        )
        print(f"Successfully processed {src_key} -> s3://{dst_bucket}/{dst_key}")
        
    return {'statusCode': 200, 'body': 'Processing completed.'}
