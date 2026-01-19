from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-principals-Iceberg-MinIO") \
    .getOrCreate()

# Read from raw data from S3
title_principals_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.principals.tsv.gz")

# Rename columns
title_principals_rename_columns_df = title_principals_df.withColumnsRenamed({"tconst": "title_id", "nconst": "name_id"})

# Load to Iceberg
title_principals_df.writeTo("demo.bronze.title_principals").createOrReplace()

spark.stop()
