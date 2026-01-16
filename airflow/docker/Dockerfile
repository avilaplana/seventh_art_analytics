# Start from official Airflow image
FROM apache/airflow:2.9.3

USER root

# Install Java (required by Spark)
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jre \
        curl \
        netcat-openbsd \
        dnsutils && \
    apt-get clean
    
# Set JAVA_HOME (required by Spark) Fix this to match architecture (e.g., amd64, arm64)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark (same version as your Spark standalone cluster)
ENV SPARK_VERSION=3.5.1
ENV SPARK_MAJOR_VERSION=3.5
ENV HADOOP_VERSION=3
ENV ICEBERG_VERSION=1.5.0

RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV PATH="/opt/spark/bin:$PATH"
ENV SPARK_HOME=/opt/spark

# Download Iceberg Spark Runtime 1.8.1 for Spark 3.5 + Scala 2.12
RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# # OPTIONAL — only if using S3 or MinIO
RUN curl -L -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

USER airflow

COPY airflow/docker/requirements.txt /requirements.txt
# FIXED: Remove --user flag
RUN pip install --no-cache-dir -r /requirements.txt
