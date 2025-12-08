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
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3

RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV PATH="/opt/spark/bin:$PATH"

# Download Iceberg Spark Runtime 1.8.1 for Spark 3.5 + Scala 2.12
RUN curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.8.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.8.1/iceberg-spark-runtime-3.5_2.12-1.8.1.jar

# OPTIONAL — only if using S3 or MinIO
RUN curl -L -o /opt/spark/jars/iceberg-aws-bundle-1.8.1.jar \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.8.1/iceberg-aws-bundle-1.8.1.jar


# Optional: copy your core-site.xml for S3 access
COPY ../../load/spark-iceberg/core-site.xml /opt/spark/conf/core-site.xml
# Copy requirements.txt FROM CORRECT CONTEXT PATH

USER airflow

COPY airflow/docker/requirements.txt /requirements.txt
# FIXED: Remove --user flag
RUN pip install --no-cache-dir -r /requirements.txt
