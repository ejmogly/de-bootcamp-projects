# 과제 05 실행 로그 실측 재현 자료 (김준님 전용)

김준님께서 요청하신 실측 재현 환경입니다.
김준님이 제출하신 **`process_stocks.py`(수정 없음)**를 김준님이 제출하신 **Dockerfile 환경**에서
세 가지 방식으로 실행하고, 제출하신 `spark_submit.txt`와 비교할 수 있도록 구성했습니다.

## 구성

| 경로 | 내용 |
|---|---|
| `env/Dockerfile`, `env/requirements.txt` | 제출하신 `Q1_2_3_realestate/` 의 Dockerfile·requirements 그대로 (apache/airflow:2.8.1 + OpenJDK 17 + pyspark 3.5.7) |
| `code/process_stocks.py` | 제출하신 `Q5/process_stocks.py` 그대로 (변경 없음) |
| `data/stock_prices.csv` | 과제 05 제공 데이터 |
| `stub/boto3.py` | AWS 자격증명 없이 실행하기 위한 boto3 대역 (아래 설명) |
| `conf_python_info/log4j2.properties` | Spark 3.5.7 공식 템플릿에 설정 2줄만 추가한 로그 설정 파일 |
| `run.sh` | 이미지 빌드 → 세 가지 실행 → 제출 로그와 자동 비교 |
| `results/` | 채점 측에서 실행한 결과 (`0_제출하신_spark_submit.txt`는 제출하신 원본) |

### boto3 대역에 대하여
`process_stocks.py`는 S3에서 데이터를 내려받고 결과를 올립니다. 채점 측에는 김준님 계정의 AWS 자격증명이 없으므로,
`stub/boto3.py`가 S3 다운로드를 `data/stock_prices.csv` 복사로, 업로드·삭제를 아무 동작 없음으로 대신합니다.
Spark 동작(SparkSession 생성, 로그 출력, 정제·UDF·집계)에는 관여하지 않습니다.
본인 AWS 자격증명으로 실제 S3를 사용해 보시려면 `run.sh`의 `export PYTHONPATH=/work/stub` 부분을 빼고 실행하시면 됩니다.
(이 경우 실제 버킷의 `stock/silver/` 객체가 삭제·재업로드되니 유의해 주세요.)

`log4j2.properties`에서 공식 템플릿과 다른 부분은 아래 2줄뿐입니다.

```properties
logger.pygw.name = org.apache.spark.api.python.PythonGatewayServer
logger.pygw.level = info
```

## 실행 방법

준비물: Docker (macOS·Linux·Windows Git Bash)

```bash
bash run.sh
```

처음 실행 시 제출하신 Dockerfile로 이미지를 빌드합니다. 결과는 `my_results/`에 저장되고, 마지막에 요약과 비교 결과가 출력됩니다.

## 세 가지 실행

| # | 실행 방식 | 컨테이너 안에서 실행한 명령 |
|---|---|---|
| 1 | spark-submit, 기본 설정 | `spark-submit /work/code/process_stocks.py 2>&1` |
| 2 | python 직접, 기본 설정 | `python /work/code/process_stocks.py 2>&1` |
| 3 | python 직접, 설정 변경 | 2번 + `SPARK_CONF_DIR=conf_python_info` + `PYSPARK_SUBMIT_ARGS='--conf spark.ui.showConsoleProgress=false pyspark-shell'` |

## 측정 결과 (`results/`)

| 로그 | 줄 수 | `Setting default log level to "WARN"` | INFO 줄 | `Submitted application` | `[Stage …]` 진행률 | 제출하신 로그와 비교 |
|---|---|---|---|---|---|---|
| 0. 제출하신 `spark_submit.txt` | 66 | 없음 | 38 | 있음 | 없음 | — |
| 1. spark-submit, 기본 설정 | 66 | 없음 | 38 | 있음 | 없음 | **동일** |
| 2. python 직접, 기본 설정 | 30 | **있음** | 0 | 없음 | **있음** | 다름 |
| 3. python 직접, 설정 변경 | 66 | 없음 | 38 | 있음 | 없음 | **동일** |

비교는 실행할 때마다 바뀌는 값(시각, 커널·CPU 정보, 호스트·포트, 임시 폴더·파일 ID, 메모리 수치)을 제외하고 한 줄씩 대조한 결과입니다.
마지막 `Uploaded:` 두 줄(`part-00000-….snappy.parquet`, `_SUCCESS`)은 `process_stocks.py` 105~106행 `os.walk`가 파일을 정렬하지 않고 읽는 순서대로 찍혀,
같은 실행 방식에서도 순서가 바뀔 수 있습니다. 그래서 비교 시 이 두 줄은 순서를 맞춰 대조합니다.

## 확인되는 사실

1. **기본 설정에서는** python 직접 실행 시 `Setting default log level to "WARN"` 메시지와 진행률 표시가 나오고, INFO 로그는 나오지 않습니다. (2번)
2. `spark_submit.txt`의 INFO 로그(`Running Spark version`, `ResourceProfile created`, `Successfully started service 'sparkDriver'`, `Initialized BlockManager` 등)는
   `process_stocks.py` **18~22행 `getOrCreate()`**가 SparkContext를 만드는 동안 출력됩니다.
   **23행 `setLogLevel("WARN")`**은 그 이후 로그에만 적용되므로, 3번처럼 python 직접 실행이어도 그 전 INFO 로그는 출력됩니다.
   두 실행 모두 INFO 로그가 `Initialized BlockManager`에서 끝나고 다음 줄이 `Loading raw data from S3: …`입니다.
3. `Submitted application` 로그는 spark-submit 전용 로그가 아니라 실행 방식과 관계없이 SparkContext가 출력하는 INFO 로그입니다. (3번에도 있음)
4. 로그 설정 파일과 진행률 표시 설정을 바꾸면, python 직접 실행도 제출하신 로그와 같은 로그를 출력합니다. (3번)
   → **실행 로그만으로는 실행 방식을 확정할 수 없습니다.**

## 참고한 Spark 3.5.7 소스

- `common/utils/src/main/scala/org/apache/spark/internal/Logging.scala` 153~171행: 대화형(셸) 모드에서 해당 로거에 별도 설정이 없으면 기본 레벨을 WARN으로 바꾸고 `Setting default log level` 메시지 출력
- `core/src/main/scala/org/apache/spark/api/python/PythonGatewayServer.scala` 34행: python 직접 실행 시 `initializeLogIfNecessary(true)`로 셸 모드 로깅 초기화
- `core/src/main/scala/org/apache/spark/SparkContext.scala` 427행: `logInfo(s"Submitted application: $appName")`
- `core/src/main/resources/org/apache/spark/log4j2-defaults.properties`: 콘솔 로그 출력 대상은 `SYSTEM_ERR`(stderr)

## 과제 판정과의 관계

과제 05의 판정은 문제지 X 판정 사례 "캡처에 실행 커맨드가 노출되지 않아 판정 불가"에 따라 **캡처에 노출된 실행 커맨드**로 확인합니다.
이 자료는 실행 로그가 왜 판정 수단이 될 수 없는지를 보여 드리기 위한 것이며, 김준님이 위와 같은 설정으로 실행하셨다는 의미가 아닙니다.
1번 결과처럼 제출하신 로그는 spark-submit 기본 실행 결과와도 일치합니다.
