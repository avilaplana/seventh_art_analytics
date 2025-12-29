from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-crew-Iceberg-MinIO") \
    .getOrCreate()

# Drop the table demo.bronze.title_crew
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_crew""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_crew (
    tconst STRING,
    directors STRING,
    writers STRING
) USING iceberg
""")

# Read from raw data from S3
title_crew_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.crew.tsv.gz")

# Load to Iceberg
title_crew_df.writeTo("demo.bronze.title_crew").createOrReplace()

spark.stop()