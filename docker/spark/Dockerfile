FROM ubuntu:22.04

# 安装 Java (Spark 必须)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl wget software-properties-common python3.10 python3.10-venv python3.10-distutils && \
    ln -sf /usr/bin/python3.10 /usr/bin/python3 && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 安装 Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3

# 设置 Spark 模式（由 docker-compose 注入）
ENV SPARK_MODE=master

# 默认命令
CMD ["/bin/bash"]
