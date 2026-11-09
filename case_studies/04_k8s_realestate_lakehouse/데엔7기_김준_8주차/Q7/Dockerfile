FROM python:3.10-slim

# OpenJDK 및 필수 유틸리티 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

# PySpark 및 AWS 연동 패키지 설치
RUN pip install --no-cache-dir pyspark==3.5.1 boto3 pyarrow

WORKDIR /app
COPY spark_job.py /app/spark_job.py

ENTRYPOINT ["python3", "/app/spark_job.py"]
