from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-episode-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.imdb.title_episode
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_episode""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_episode (
    title_id STRING,
    parent_title_id STRING,
    season_number STRING,
    episode_number STRING
) USING iceberg
""")

# Read from raw data from S3
title_episode_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.episode.tsv.gz")

# Rename columns
title_episode_rename_columns_df = title_episode_df.withColumnsRenamed({"tconst": "title_id", "parentTconst": "parent_title_id", "seasonNumber": "season_number", "episodeNumber": "episode_number"})

# Set NULL when \N is found
title_episode_set_null_values_df = title_episode_rename_columns_df \
    .withColumn("season_number",when(col("season_number") == "\\N", None).otherwise(col("season_number"))) \
    .withColumn("episode_number",when(col("episode_number") == "\\N", None).otherwise(col("episode_number")))

# Load to Iceberg
title_episode_set_null_values_df.writeTo("demo.imdb.title_episode").createOrReplace()


