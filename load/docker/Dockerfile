FROM bitnamilegacy/spark:3.5.5

ENV SPARK_VERSION=3.5.5
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.8.0

ENV SPARK_HOME=/opt/bitnami/spark

# Add Hadoop AWS + AWS SDK jars (versions must match Hadoop in the base image, likely 3.3.x)
RUN curl -s https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
-Lo ${SPARK_HOME}/jars/hadoop-aws-3.3.4.jar && \
curl -s https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar \
-Lo ${SPARK_HOME}/jars/aws-java-sdk-bundle-1.11.1026.jar && \
curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar \
-Lo ${SPARK_HOME}/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar && \
curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
-Lo ${SPARK_HOME}/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

COPY load/docker/core-site.xml ${SPARK_HOME}/conf/core-site.xml
COPY load/docker/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf


