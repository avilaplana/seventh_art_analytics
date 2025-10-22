from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-akas-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.imdb.title_akas
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_akas""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_akas (
    title_id STRING,
    ordering STRING,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    is_original_title STRING
) USING iceberg
""")

# Read from raw data from S3
title_akas_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.akas.tsv.gz")

# Rename columns
title_akas_rename_columns_df = title_akas_df.withColumnsRenamed({"titleId": "title_id", "isOriginalTitle": "is_original_title"})

# Set NULL when \N is found
title_akas_set_null_values_df = title_akas_rename_columns_df \
    .withColumn("region",when(col("region") == "\\N", None).otherwise(col("region"))) \
    .withColumn("language",when(col("language") == "\\N", None).otherwise(col("language"))) \
    .withColumn("types",when(col("types") == "\\N", None).otherwise(col("types"))) \
    .withColumn("attributes",when(col("attributes") == "\\N", None).otherwise(col("attributes")))

# Load to Iceberg
title_akas_set_null_values_df.writeTo("demo.imdb.title_akas").createOrReplace()

spark.stop()