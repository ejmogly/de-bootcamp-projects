from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="run_spark_job",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["Q2", "과제02"],
) as dag:

    # SparkSubmitOperator로 analyze_csv.py 실행 (문제 요구조건)
    submit_spark_job = SparkSubmitOperator(
        task_id="submit_analyze_csv_job",
        application="/workspace/analyze_csv.py",
        conn_id="spark_default",
        verbose=True,
    )

    submit_spark_job
