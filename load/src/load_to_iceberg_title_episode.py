from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-episode-Iceberg-MinIO") \
    .getOrCreate()

# Read from raw data from S3
title_episode_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.episode.tsv.gz")

# Load to Iceberg
title_episode_df.writeTo("demo.bronze.title_episode").createOrReplace()

spark.stop()
