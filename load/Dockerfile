FROM tabulario/iceberg-rest:latest

USER root
WORKDIR /usr/lib/iceberg-rest

# Download S3 dependencies
RUN apt-get update && apt-get install -y curl && apt-get install -y unzip && rm -rf /var/lib/apt/lists/* \
 && curl -L -o hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -L -o aws-java-sdk-bundle-1.12.550.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.550/aws-java-sdk-bundle-1.12.550.jar

# Add the jars to CLASSPATH used by the original entrypoint
CMD ["java", "-cp", "/usr/lib/iceberg-rest/iceberg-rest-image-all.jar:/usr/lib/iceberg-rest/hadoop-aws-3.3.4.jar:/usr/lib/iceberg-rest/aws-java-sdk-bundle-1.12.550.jar", "org.apache.iceberg.rest.RESTCatalogServer"]

USER 1000