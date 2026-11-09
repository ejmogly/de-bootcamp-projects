from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="price_pipeline_dag",
    default_args=default_args,
    description="식료품 가격 배치 파이프라인 (Kafka -> Spark -> S3 Medallion -> Athena)",
    schedule_interval="@daily",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    tags=["week9", "grocery_prices", "spark", "athena"],
) as dag:

    # 1. SparkSubmitOperator로 price_batch.py 실행 (0점 방지: BashOperator 금지)
    run_price_batch = SparkSubmitOperator(
        task_id="run_price_batch",
        application="/home/ubuntu/part2-price/price_batch.py",
        conn_id="spark_default",
        conf={"spark.master": "spark://spark-master:7077"},
        verbose=True,
    )

    # 2. Athena 테이블 등록 태스크
    CREATE_ATHENA_TABLE_SQL = """
    CREATE EXTERNAL TABLE IF NOT EXISTS grocery_price_agg (
        item_name STRING,
        price_nov DOUBLE,
        price_dec DOUBLE,
        change_rate DOUBLE
    )
    STORED AS PARQUET
    LOCATION 's3://de7-junekim-week9/gold/grocery_price_agg/'
    TBLPROPERTIES ('parquet.compression'='SNAPPY');
    """

    register_athena_table = AthenaOperator(
        task_id="register_athena_table",
        query=CREATE_ATHENA_TABLE_SQL,
        database="de9week_price_db",
        output_location="s3://de7-junekim-week9/athena-results/",
        aws_conn_id="aws_default",
    )

    # 3. Athena 쿼리 검증 태스크
    VERIFY_QUERY_SQL = """
    SELECT item_name, price_nov, price_dec, change_rate
    FROM grocery_price_agg
    ORDER BY abs(change_rate) DESC
    LIMIT 3;
    """

    verify_athena_query = AthenaOperator(
        task_id="verify_athena_query",
        query=VERIFY_QUERY_SQL,
        database="de9week_price_db",
        output_location="s3://de7-junekim-week9/athena-results/",
        aws_conn_id="aws_default",
    )

    run_price_batch >> register_athena_table >> verify_athena_query
