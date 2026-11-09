from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="hello_airflow_dag",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["Q1", "과제01"],
) as dag:

    print_hello = BashOperator(
        task_id="print_hello",
        bash_command="echo 'Hello Airflow'",
    )

    print_hello
