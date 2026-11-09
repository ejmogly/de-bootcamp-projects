FROM apache/airflow:2.8.1

USER root
# 1. Java (OpenJDK 17) 설치
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. JAVA_HOME 설정 (아키텍처 호환)
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 3. Airflow 사용자로 복귀 및 파이썬 패키지 설치
USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
