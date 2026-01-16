FROM tabulario/spark-iceberg:3.5.1_1.5.0

# Add Hadoop AWS + AWS SDK jars (versions must match Hadoop in the base image, likely 3.3.x)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/

# Optional: verify they’re present
RUN ls -lh /opt/spark/jars/hadoop-aws-*.jar /opt/spark/jars/aws-java-sdk-bundle-*.jar

COPY load/docker/core-site.xml /opt/spark/conf/core-site.xml
COPY load/docker/spark-defaults.conf /opt/spark/conf/spark-defaults.conf