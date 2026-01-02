# Start from Ubuntu 22.04 for arm64 compatibility
FROM ubuntu:22.04

# Install basic utilities and Java
RUN apt-get update && \
    apt-get install -y \
        openjdk-17-jre-headless \
        python3 \
        python3-pip \
        python3-venv \
        curl \
        netcat \
        dnsutils \
        git \
        unzip && \
    apt-get clean

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark
ENV SPARK_VERSION=3.5.5
ENV HADOOP_VERSION=3
RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    | tar -xz -C /opt/ && \
    ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark
ENV PATH="/opt/spark/bin:$PATH"

# Create a non-root user
RUN useradd -ms /bin/bash dbt
USER dbt
WORKDIR /home/dbt

# Copy requirements and install
COPY docker/requirements.txt .
RUN pip3 install --no-cache-dir --user -r requirements.txt

# Add local user bin to PATH
ENV PATH=/home/dbt/.local/bin:$PATH

# Copy dbt profiles
COPY docker/profiles.yml /home/dbt/.dbt/profiles.yml

# Mount point for your project
VOLUME ["/home/dbt/medallion"]
WORKDIR /home/dbt/medallion

# Default command
CMD ["dbt", "--version"]
