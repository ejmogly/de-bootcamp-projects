"""Redshift Data API 로 S3 parquet 을 적재하는 모듈 (과제 07).

- 적재는 staging 테이블에 COPY 한 뒤 본 테이블을 교체하는 방식으로, 전 과정을 하나의 트랜잭션
  (batch_execute_statement)으로 실행한다. 중간에 실패해도 기존 데이터가 남는다.
- 상태 조회는 지수 백오프로 대기하고, 제한 시간을 넘기면 예외를 던진다(무한 대기 방지).
- Data API 는 HTTP 호출이라 DB 커넥션 풀을 직접 관리할 필요가 없다.
"""
import time
from dataclasses import dataclass

TERMINAL_OK = "FINISHED"
TERMINAL_FAIL = ("FAILED", "ABORTED")


class RedshiftLoadError(RuntimeError):
    pass


@dataclass
class RedshiftDataLoader:
    client: object                 # boto3.client("redshift-data")
    workgroup: str
    database: str
    timeout_sec: float = 600.0
    initial_wait_sec: float = 1.0
    max_wait_sec: float = 10.0
    sleep = staticmethod(time.sleep)

    def run_transaction(self, sqls: list) -> str:
        """여러 SQL 을 하나의 트랜잭션으로 실행하고 statement id 를 돌려준다."""
        resp = self.client.batch_execute_statement(
            WorkgroupName=self.workgroup, Database=self.database, Sqls=sqls
        )
        self._wait(resp["Id"])
        return resp["Id"]

    def query_scalar(self, sql: str) -> int:
        """단일 값(예: COUNT)을 조회한다."""
        resp = self.client.execute_statement(
            WorkgroupName=self.workgroup, Database=self.database, Sql=sql
        )
        self._wait(resp["Id"])
        result = self.client.get_statement_result(Id=resp["Id"])
        return result["Records"][0][0]["longValue"]

    def _wait(self, statement_id: str) -> None:
        waited, delay = 0.0, self.initial_wait_sec
        while True:
            desc = self.client.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status == TERMINAL_OK:
                return
            if status in TERMINAL_FAIL:
                raise RedshiftLoadError(f"{status}: {desc.get('Error', '')}")
            if waited >= self.timeout_sec:
                raise RedshiftLoadError(f"timeout after {waited:.0f}s (last status={status})")
            self.sleep(delay)
            waited += delay
            delay = min(delay * 2, self.max_wait_sec)


def replace_table_sqls(table: str, ddl_columns: str, s3_uri: str, iam_role_arn: str) -> list:
    """staging COPY → 본 테이블 교체 SQL 목록 (한 트랜잭션으로 실행할 것)."""
    stage = f"{table}_stage"
    return [
        f"CREATE TABLE IF NOT EXISTS {table} ({ddl_columns})",
        f"CREATE TEMP TABLE {stage} (LIKE {table})",
        f"COPY {stage} FROM '{s3_uri}' IAM_ROLE '{iam_role_arn}' FORMAT AS PARQUET",
        f"DELETE FROM {table}",
        f"INSERT INTO {table} SELECT * FROM {stage}",
    ]
