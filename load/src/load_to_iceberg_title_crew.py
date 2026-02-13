from pyspark.sql import SparkSession
from s3_utils import object_path
from pyspark.sql.functions import current_date

spark = SparkSession.builder \
    .appName("load-title-crew-Iceberg-MinIO") \
    .getOrCreate()

# Read from raw data from S3
title_crew_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.crew.tsv.gz")

# Load to Iceberg
title_crew_df \
    .withColumn("ingestion_date", current_date()) \
    .writeTo("demo.bronze.title_crew") \
    .overwritePartitions()

spark.stop()