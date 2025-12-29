from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-name-basics-Iceberg-MinIO") \
    .getOrCreate()

# Drop table demo.bronze.name_basics
spark.sql("""DROP TABLE IF EXISTS demo.bronze.name_basics""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.name_basics (
    nconst STRING,
    primaryName STRING,
    birthYear STRING,
    deathYear STRING,
    primaryProfession STRING,
    knownForTitles STRING
) USING iceberg
""")

# Read from raw data from S3/MinIO
name_basics_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/name.basics.tsv.gz")

# Load to Iceberg
name_basics_df.writeTo("demo.bronze.name_basics").createOrReplace()

spark.stop()