FROM apache/airflow:2.9.1-python3.10

ENV AIRFLOW_HOME=/opt/airflow
ENV GCLOUD_HOME=/home/google-cloud-sdk
ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER root

RUN apt-get update -qq && apt-get install -y \
    vim \
    curl \
    gnupg \
    gosu \
    && apt-get clean

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

USER root

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
