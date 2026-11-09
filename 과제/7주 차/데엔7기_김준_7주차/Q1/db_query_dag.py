from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

def query_postgres_db():
    # 1. Variable.get으로 테이블명 로딩 (문제 요구조건)
    table_name = Variable.get("sample_table_name", default_var="sample_users")
    print(f"[INFO] Loaded table name from Airflow Variable: {table_name}")

    # 2. PostgresHook으로 PostgreSQL 연결 (문제 요구조건)
    hook = PostgresHook(postgres_conn_id="postgres_default")

    # 3. SELECT 쿼리 실행 및 결과 출력
    sql = f"SELECT * FROM {table_name};"
    records = hook.get_records(sql)

    print(f"[SUCCESS] Executed SQL query: '{sql}'")
    print(f"[SUCCESS] Query returned {len(records)} rows:")
    for row in records:
        print(f"  -> {row}")

with DAG(
    dag_id="db_query_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["Q1", "과제01"],
) as dag:

    query_task = PythonOperator(
        task_id="query_sample_table",
        python_callable=query_postgres_db,
    )

    query_task
