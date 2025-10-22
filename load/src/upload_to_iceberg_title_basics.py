from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path
spark = SparkSession.builder \
    .appName("load-title-basics-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.imdb.title_basics
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_basics""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_basics (
    title_id STRING,
    title_type STRING,
    primary_title STRING,
    original_title STRING,
    is_adult STRING,
    start_year STRING,
    end_year STRING,
    runtime_minutes STRING,
    genres STRING
) USING iceberg
""")

# Read from raw data from S3
title_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.basics.tsv.gz")


# Rename columns# Rename columns
title_rename_columns_df = title_df.withColumnsRenamed({"tconst": "title_id", "titleType": "title_type", "primaryTitle": "primary_title", "originalTitle": "original_title", "isAdult": "is_adult", "startYear": "start_year", "endYear": "end_year", "runtimeMinutes": "runtime_minutes"})

# Set NULL when \N is found
title_basics_set_null_values_df = title_rename_columns_df \
    .withColumn("end_year",when(col("end_year") == "\\N", None).otherwise(col("end_year")))


# Load to Iceberg# Load to Iceberg
title_basics_set_null_values_df.writeTo("demo.imdb.title_basics").createOrReplace()
