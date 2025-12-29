from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-akas-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.bronze.title_akas
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_akas""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_akas (
    titleId STRING,
    ordering STRING,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    isOriginalTitle STRING
) USING iceberg
""")

# Read from raw data from S3
title_akas_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.akas.tsv.gz")

# Load to Iceberg
title_akas_df.writeTo("demo.bronze.title_akas").createOrReplace()

spark.stop()