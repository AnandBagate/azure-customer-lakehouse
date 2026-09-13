FROM apache/airflow:3.1.0

USER root

# Install Java 17
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install PySpark and Delta Lake
RUN pip install --no-cache-dir \
    pyspark==3.5.3 \
    delta-spark==3.2.0