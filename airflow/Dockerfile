# Extends the stock Airflow image with what's needed to submit jobs to the
# Spark cluster (spark-master:7077) over the network - no Docker socket
# mounting, no docker-exec-into-another-container tricks. Just a normal
# network client, which is far more reliable across Windows/Mac/Linux Docker
# setups than socket-based approaches.
FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt /requirements.txt
# pyspark version is pinned to MATCH the bitnami/spark:3.5 cluster image
# (docker-compose.yml). A mismatched client/cluster Spark version is a
# classic source of confusing spark-submit failures - keep these in sync
# if you ever bump the Spark version in docker-compose.yml.
RUN pip install --no-cache-dir --user -r /requirements.txt
