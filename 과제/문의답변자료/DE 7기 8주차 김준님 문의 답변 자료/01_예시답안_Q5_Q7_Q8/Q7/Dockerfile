# 과제 07 Spark 이미지
# 빌드는 01_예시답안_Q5_Q7_Q8 폴더에서 실행한다 (과제 05 모듈을 함께 담기 위해):
#   docker build -f Q7/Dockerfile -t de7-q7-spark:1.0.0 .
FROM apache/spark:3.5.7-python3

USER root
# S3A 커넥터 (Spark 3.5.x 의 Hadoop 3.3.4 와 맞는 버전)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
# URL 로 받은 파일은 권한이 600 이라 spark(uid 185) 가 읽을 수 있게 바꾼다.
# (ADD --chmod 는 BuildKit 전용이라 Ubuntu docker.io 기본 빌더에서 실패하므로 RUN 으로 처리)
RUN chmod 644 /opt/spark/jars/hadoop-aws-3.3.4.jar /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar  && pip install --no-cache-dir boto3==1.35.*

COPY Q5/process_stocks.py Q7/redshift_loader.py Q7/spark_job.py /opt/spark/work-dir/
ENV PYTHONPATH=/opt/spark/work-dir

# 공식 이미지의 실행 사용자(spark, uid 185)로 되돌린다.
USER 185
