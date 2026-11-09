import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def download_from_s3():
    bucket_name = "de-7-junekim"
    s3_key = "data.csv"
    local_path = "/opt/airflow/data/data.csv"

    # 1. Airflow에 등록된 aws_default 연결을 통해 S3Hook 호출
    hook = S3Hook(aws_conn_id="aws_default")
    s3_client = hook.get_conn()

    # 2. 로컬 디렉터리 준비 및 S3 파일 다운로드
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    s3_client.download_file(bucket_name, s3_key, local_path)

    file_size = os.path.getsize(local_path)
    print(f"[SUCCESS] Successfully downloaded s3://{bucket_name}/{s3_key} to {local_path}")
    print(f"[SUCCESS] Downloaded file size: {file_size} bytes")

with DAG(
    dag_id="s3_download_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["Q1", "과제01"],
) as dag:

    download_task = PythonOperator(
        task_id="download_s3_data",
        python_callable=download_from_s3,
    )

    download_task
