
[Mastering Spark] 데이터 엔지니어링 비법 요약 노트
1. 강의 핵심 요약 (Executive Summary)
본 강의는 분산 데이터 처리의 표준인 Apache Spark를 활용하여 대규모 데이터를 정제하고 파이프라인을 구축하는 실무 능력을 기르는 데 목표를 둡니다.

엔지니어링의 본질: 단순히 데이터를 옮기는 것이 아니라, 효율적인 데이터 저장소 관리와 정제 과정을 통해 사이언티스트가 분석 가능한 환경을 제공하는 것.
하둡에서 스파크로: 하둡 맵리듀스의 디스크 I/O 병목 문제를 메모리 기반 처리(In-memory)로 해결하여 성능을 100배 이상 향상.
스파크 아키텍처: 드라이버(마스터), 익스큐터(워커), 클러스터 매니저로 구성된 분산 처리 구조를 이해하고, RDD에서 데이터프레임으로 진화한 데이터 처리 모델의 변화를 파악.
실무 역량: 최적화(셔플링 최소화, 캐싱, 파티셔닝), 스트리밍 데이터 처리(Structured Streaming), 그리고 서드파티 라이브러리(MLlib, GraphX) 연동 및 운영 역량 확보.
2. 개념 및 용어 사전 (Glossary)
용어  정의  강의 내 핵심 맥락
RDD Resilient Distributed Dataset   스파크의 근간이 되는 데이터 모델. 회복 탄력성(유실 시 복구)과 불변성 보유.
카탈리스트(Catalyst) 스파크 SQL의 쿼리 최적화 엔진  논리적 계획을 물리적 계획으로 전환할 때 최적의 경로를 자동으로 선택.
셔플링(Shuffling)  파티션 간 데이터 재분배 작업    조인, 그룹화 연산 시 데이터 이동이 발생하며 성능 저하의 주범.
파티셔닝(Partitioning)  데이터 분할 단위   작업의 병렬성을 결정. 너무 작으면 유휴 리소스 발생, 너무 크면 OOM(메모리 부족) 발생.
텅스텐(Tungsten)   스파크의 저수준 메모리 관리 프로젝트    자바 객체 대신 이진 인코딩을 사용하여 메모리 오버헤드와 GC 부하 감소.
워터마킹(Watermarking)  스트리밍 데이터 폐기 기한 설정   스트리밍에서 특정 시간이 지난 과거 데이터를 삭제하여 메모리 보호.
UDF (User Defined Function) 사용자 정의 함수   내장 함수로 해결 안 되는 복잡한 로직을 파이썬/스칼라로 직접 정의하여 사용.
3. 코드 및 실무 스크립트 치트 시트 (Code Cheat Sheet)
1) Spark Session 생성 (표준 방식)
pythonfrom pyspark.sql import SparkSession

# SparkSession은 스파크의 모든 기능(SQL, RDD, Dataframe)을 아우르는 래퍼 객체
spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.some.config.option", "value") \
    .getOrCreate()
2) 데이터 처리와 파티셔닝 최적화
python# 1. 셔플링을 유발하지 않는 효율적 필터링
df.filter(F.col("col_name") == "target")

# 2. 파티션 제어: 데이터가 작을 때 성능 최적화를 위해 파티션 개수 축소
df.coalesce(1).write.csv("output_path")

# 3. 데이터 로컬 캐싱: 여러 번 호출될 데이터는 메모리에 올려둠
df.cache() 
3) 스트리밍 데이터 실무 (Structured Streaming)
python# 스트리밍 소스 읽기 (테이블 형태의 언바운디드 데이터로 취급)
raw_df = spark.readStream.format("json").schema(schema).load("/data/events")

# 윈도우 기반 집계
result_df = raw_df.groupBy(F.window(F.col("time"), "10 minutes"), "action").count()

# 콘솔 출력 (아웃풋 모드 설정)
query = result_df.writeStream.outputMode("complete").format("console").start()
query.awaitTermination()
4. 강사의 실무 꿀팁 및 주의사항 (Instructor's Insights)
최적화의 정석: 정답은 없으나 다음 루틴을 반복하라. 데이터 확인 -> DAG(실행계획) 확인 -> 리소스(메모리) 조정 -> 코드 로직 최적화.
데이터 스큐(Data Skew) 예방: 특정 파티션에만 데이터가 몰리는 '데이터 스큐'는 전체 작업을 지연시킨다. 키 값의 분포를 확인하고 repartition을 적절히 활용할 것.
조인(Join) 주의: 대규모 데이터 조인은 셔플링을 유발하는 무거운 연산이다. 작은 테이블과 큰 테이블 조인 시 **브로드캐스트 조인(Broadcast Join)**을 사용하여 네트워크 이동을 최소화하라.
스파크 서브밋(spark-submit): 실무에서는 노트북보다 파이썬 스크립트(.py)로 작성 후 서브밋하여 운영한다. 이때 메모리 설정(--executor-memory, --driver-memory)을 클러스터 환경에 맞게 직접 명시하라.
체크포인트(Checkpoint): 재계산 비용이 너무 큰 복잡한 연산 체인 중간에는 checkpoint()를 사용하여 디스크에 상태를 저장하고 거기서부터 다시 시작할 수 있게 하라.


📘 Data Engineering Master: Spark Optimization & Internal Deep Dive
1. 강의 핵심 요약 (Executive Summary)
본 강의는 스파크 코어 모듈을 넘어 실제 실무 환경에서의 써드파티 연동(제플린), 라이브러리(GraphFrames) 의존성 관리를 학습합니다. 핵심 목표는 다음과 같습니다.

실무 워크플로우: 데이터 로드-처리-모델링-시각화로 이어지는 캡스톤 프로젝트 수행.
성능 최적화: 카탈리스트 옵티마이저를 활용한 Logical/Physical Plan 해석법 습득.
내부 구조 파악: RDD와 DataFrame의 내부 동작 원리(Lazy Evaluation, Dependency) 심층 분석.
엔지니어링 역량: 내부 구조 이해를 바탕으로 효율적인 데이터 처리 코드를 설계하는 능력 함양.
2. 개념 및 용어 사전 (Glossary)
용어  뜻   활용 맥락
Zeppelin    인터랙티브 노트북 도구    주피터 대비 시각화와 스칼라/스파크 연동이 용이함
Catalyst Optimizer  스파크 SQL 최적화 엔진  쿼리를 분석하여 효율적인 물리적 실행 계획으로 변환
RDD 분산 데이터셋 기본 단위   스파크의 가장 기초적인 데이터 모델이자 실행 체인의 뿌리
Logical Plan    추상적 실행 계획   데이터 처리의 논리적 순서가 정의된 연산 트리
Predicate Pushdown  필터 최적화 기법   필터링을 데이터 소스단으로 밀어 넣어 I/O 비용 감소
Wide Dependency 셔플 발생 의존성   데이터 파티션 간 이동이 필요하여 스테이지가 분리되는 지점
Row Encoder 인코딩 최적화 모듈  데이터프레임 행 데이터를 효율적으로 직렬화하여 성능 보장
3. 코드 및 실무 스크립트 치트 시트 (Code Cheat Sheet)
3.1 외부 라이브러리(Jar) 의존성 추가
python# Spark Session 설정 - 세션 시작 시 외부 라이브러리 로드
spark = SparkSession.builder \
    .config("spark.driver.extraClassPath", "/path/to/lib.jar") \
    .config("spark.jars", "/path/to/lib.jar") \
    .getOrCreate()
# [주석] 로컬 환경에서 드라이버와 실행기(Executor)가 JAR 파일을 인식하게 설정함

# Spark-submit 실행 옵션
--packages graphframes:graphframes:0.8.2-spark3.x-s_2.12 \
--jars /path/to/graphframes.jar
# [주석] 종속성을 자동으로 다운로드하고 명시적 경로의 라이브러리를 연결
3.2 쿼리 실행 계획 확인
python# 실행 계획 시각화
df.explain() 
# [주석] 릴레이션 -> 필터 -> 프로젝트 순의 실행 흐름을 확인하여 최적화 지점 찾기
4. 강사의 실무 꿀팁 및 주의사항 (Instructor's Insights)
Explain()을 생활화하라: 느린 코드의 90%는 실행 계획이 비효율적인 경우임. explain()을 통해 필터 조건이 조인보다 뒤에 있는지 확인하라.
트리 구조는 하단(Leaf)부터 해석: 실행 계획 트리는 아래에서 위로 올라오며 데이터가 로드되고 필터링되는 구조를 가짐.
불필요한 컬럼 I/O 차단: select 구문을 통해 필요한 컬럼만 정의하는 'Projection Pruning'만으로도 메모리 사용량을 크게 줄일 수 있음.
셔플(Shuffle)을 최소화하라: Wide Dependency가 발생하는 곳이 스테이지 분리의 기준임. 불필요한 재분할(Repartition)은 금물.
직렬화(Serialization) 에러 체크: 익스큐터에서 실행되는 함수가 직렬화되지 않으면 오류가 발생함. 항상 데이터 이동을 염두에 둘 것.