from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-crew-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.imdb.title_crew
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_crew""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_crew (
    title_id STRING,
    director_id STRING,
    writers STRING
) USING iceberg
""")

# Read from raw data from S3
title_crew_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.crew.tsv.gz")

# Rename columns
title_crew_rename_columns_df = title_crew_df.withColumnsRenamed({"tconst": "title_id", "directors": "director_id"})

# Set NULL when \N is found
title_crew_set_null_values_df = title_crew_rename_columns_df \
    .withColumn("writers",when(col("writers") == "\\N", None).otherwise(col("writers")))

# Load to Iceberg
title_crew_set_null_values_df.writeTo("demo.imdb.title_crew").createOrReplace()
