from datetime import datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'ejay',
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}

@dag(
    dag_id='taskflow_etl_pipeline',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['airflow_3', 'taskflow_api']
)
def taskflow_etl():
    @task
    def extract_data():
        return {'total_orders': 1500, 'date': '2026-08-25'}

    @task
    def transform_data(raw_data: dict):
        raw_data['processed'] = True
        return raw_data

    @task
    def load_data(transformed: dict):
        print(f"Loaded: {transformed}")

    raw = extract_data()
    tf = transform_data(raw)
    load_data(tf)

taskflow_etl_dag = taskflow_etl()
