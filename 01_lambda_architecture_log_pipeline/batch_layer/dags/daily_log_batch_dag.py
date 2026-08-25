from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'ejay',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='daily_clinical_log_batch_pipeline',
    default_args=default_args,
    description='5.2M log batch aggregation and serving pipeline',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['lambda_architecture', 'batch_layer', 'spark', 'postgres']
) as dag:

    # Task 1: Check Data Availability
    check_raw_data = PostgresOperator(
        task_id='check_serving_db_connection',
        postgres_conn_id='postgres_default',
        sql='SELECT 1;'
    )

    # Task 2: Submit PySpark Batch Aggregation
    run_spark_batch = SparkSubmitOperator(
        task_id='run_spark_batch_aggregation',
        application='/opt/airflow/jobs/spark_batch_aggregator.py',
        conn_id='spark_default',
        application_args=['{{ ds }}'],
        executor_memory='2g',
        executor_cores=2,
        num_executors=4,
        verbose=True
    )

    # Task 3: Refresh Materialized Views in Serving Layer
    refresh_serving_views = PostgresOperator(
        task_id='refresh_lambda_unified_views',
        postgres_conn_id='postgres_default',
        sql="""
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_popular_documents;
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_category_kpi_summary;
        """
    )

    check_raw_data >> run_spark_batch >> refresh_serving_views
