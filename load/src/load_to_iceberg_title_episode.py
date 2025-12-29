from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-episode-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.bronze.title_episode
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_episode""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_episode (
    tconst STRING,
    parentTconst STRING,
    seasonNumber STRING,
    episodeNumber STRING
) USING iceberg
""")

# Read from raw data from S3
title_episode_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.episode.tsv.gz")

# Load to Iceberg
title_episode_df.writeTo("demo.bronze.title_episode").createOrReplace()

spark.stop()
