from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path
spark = SparkSession.builder \
    .appName("load-title-basics-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.bronze.title_basics
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_basics""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_basics (
    tconst STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult STRING,
    startYear STRING,
    endYear STRING,
    runtimeMinutes STRING,
    genres STRING
) USING iceberg
""")

# Read from raw data from S3
title_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.basics.tsv.gz")

# Load to Iceberg
title_df.writeTo("demo.bronze.title_basics").createOrReplace()

spark.stop()