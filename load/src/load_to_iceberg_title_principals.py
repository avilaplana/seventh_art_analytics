from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-principals-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.imdb.title_principals
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_principals""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_principals (
    title_id STRING,
    ordering STRING,
    name_id STRING,
    category STRING,
    job STRING,
    characters STRING
) USING iceberg
""")

# Read from raw data from S3
title_principals_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.principals.tsv.gz")

# Rename columns
title_principals_rename_columns_df = title_principals_df.withColumnsRenamed({"tconst": "title_id", "nconst": "name_id"})

# Set NULL when \N is found
title_principals_set_null_values_df = title_principals_rename_columns_df \
    .withColumn("job",when(col("job") == "\\N", None).otherwise(col("job"))) \
    .withColumn("characters",when(col("characters") == "\\N", None).otherwise(col("characters")))

# Load to Iceberg
title_principals_set_null_values_df.writeTo("demo.imdb.title_principals").createOrReplace()

spark.stop()
