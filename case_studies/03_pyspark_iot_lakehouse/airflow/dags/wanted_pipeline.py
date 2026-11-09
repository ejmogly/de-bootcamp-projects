from datetime import datetime, timedelta
import pandas as pd
import boto3
import os
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "de7-assignment-junekim-2026")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-northeast-2")

default_args = {
    'owner': 'JuneKim',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="wanted_job_postings_pipeline",
    default_args=default_args,
    schedule="0 9 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=True,  # 과거 3일치 Backfill 실행
    tags=["de_assignment", "wanted", "redshift"]
)
def wanted_pipeline():

    @task
    def extract(ds=None):
        """문제 8: raw 데이터 S3 업로드"""
        file_path = "/opt/airflow/data/job_postings.csv"
        df = pd.read_csv(file_path)
        print(f"[Extract] 총 로딩 건수: {len(df)} 건")

        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY if AWS_ACCESS_KEY else None, aws_secret_access_key=AWS_SECRET_KEY if AWS_SECRET_KEY else None, region_name=AWS_REGION)
        s3_key = f"raw/{ds}/job_postings.json"

        json_data = df.to_json(orient="records", lines=True, force_ascii=False)
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json_data)

        # 업로드 검증 (ContentLength, LastModified 출력)
        head = s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        print(f"[Extract 완료] Key: {s3_key}, LastModified: {head['LastModified']}, ContentLength: {head['ContentLength']}")
        return s3_key

    @task
    def transform(s3_raw_key: str, ds=None, **context):
        """문제 9: 데이터 정제 (마감일 체크, 중복 제거, null 제거, 컬럼 추출)"""
        # ==========================================================
        # [Retry 검증용 테스트 트리거]
        # 1회차 실행 시 의도적 일시 실패 ➔ Retrying 로그 생성 ➔ 2회차 자동 성공
        # ==========================================================
        ti = context.get('task_instance')
        if ti and getattr(ti, 'try_number', 1) == 1 and os.getenv("ENABLE_RETRY_TEST") == "1":
            print("[Retry 검증] 1회차 일시적 네트워크 에러 시뮬레이션 -> Retrying 동작 검증")
            raise ConnectionError("네트워크 일시적 지연 (Retry 동작 검증 테스트)")

        s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY if AWS_ACCESS_KEY else None, aws_secret_access_key=AWS_SECRET_KEY if AWS_SECRET_KEY else None, region_name=AWS_REGION)
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_raw_key)
        df = pd.read_json(obj['Body'], lines=True)

        raw_count = len(df)

        # 1. 필수 컬럼 null 제거
        df = df.dropna(subset=['id', 'title', 'company', 'job_category'])

        # 2. id 기준 중복 제거
        df = df.drop_duplicates(subset=['id'])

        # 3. 마감일자 체크 (만료된 공고 제거)
        if 'due_date' in df.columns:
            df['due_date'] = pd.to_datetime(df['due_date'], errors='coerce')
            df = df[df['due_date'].notnull()]
            df = df[df['due_date'] >= pd.to_datetime(ds)]

        # 4. 컬럼 추출 (문제 9 요구사항 명시적 반영)
        target_columns = ['id', 'title', 'company', 'location', 'job_category', 'due_date']
        df = df[[col for col in target_columns if col in df.columns]]

        clean_count = len(df)
        print(f"[Transform] 정제 전 건수: {raw_count} 건 ➔ 정제 후 건수: {clean_count} 건")

        s3_proc_key = f"processed/{ds}/clean_job_postings.csv"
        csv_data = df.to_csv(index=False)
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_proc_key, Body=csv_data)

        head = s3.head_object(Bucket=BUCKET_NAME, Key=s3_proc_key)
        print(f"[Transform 완료] Key: {s3_proc_key}, LastModified: {head['LastModified']}, ContentLength: {head['ContentLength']}")
        return s3_proc_key

    @task
    def load(s3_proc_key: str):
        """문제 10: Redshift 테이블 생성, TRUNCATE 후 COPY 및 직군별 집계 쿼리 실행"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS job_postings (
            id VARCHAR(256),
            title VARCHAR(512),
            company VARCHAR(256),
            location VARCHAR(256),
            job_category VARCHAR(128),
            due_date DATE
        );
        TRUNCATE TABLE job_postings; -- 멱등성 보장
        """
        copy_sql = f"""
        COPY job_postings
        FROM 's3://{BUCKET_NAME}/{s3_proc_key}'
        ACCESS_KEY_ID '{AWS_ACCESS_KEY}'
        SECRET_ACCESS_KEY '{AWS_SECRET_KEY}'
        IGNOREHEADER 1
        CSV;
        """
        query_sql = """
        SELECT job_category, COUNT(*) as cnt 
        FROM job_postings
        GROUP BY job_category 
        ORDER BY cnt DESC;
        """

        try:
            hook = RedshiftSQLHook(redshift_conn_id="redshift_default")
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute(create_table_sql)
            cursor.execute(copy_sql)
            cursor.execute(query_sql)
            results = cursor.fetchall()

            print("=== Redshift 직군별 공고 수 집계 결과 ===")
            for row in results:
                print(f"직군: {row[0]:<20} | 공고 수: {row[1]} 건")

            cursor.close()
            conn.close()
        except Exception as e:
            print(f"[Load 알림] Redshift 연결 정보 확인 필요: {e}")

    # Task 연결
    raw_key = extract()
    proc_key = transform(raw_key)
    load(proc_key)

wanted_dag = wanted_pipeline()
