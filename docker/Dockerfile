FROM apache/airflow:2.9.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER root

# 安装依赖
RUN apt-get update -qq && apt-get install -y \
    openjdk-17-jdk-headless \
    procps \
    vim \
    curl \
    wget \
    gnupg \
    gosu \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# back to airflow user
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN printf '#!/usr/bin/env bash\npython -m great_expectations.cli "$@"\n' > /usr/local/bin/gx \
    && chmod +x /usr/local/bin/gx

# install Google Cloud SDK
ARG CLOUD_SDK_VERSION=426.0.0
RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" -o "${TMP_DIR}/gcloud.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/gcloud.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

COPY scripts scripts
RUN chmod -R +x scripts

WORKDIR $AIRFLOW_HOME
USER $AIRFLOW_UID