from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-name-basics-Iceberg-MinIO") \
    .getOrCreate()

# Read from raw data from S3/MinIO
name_basics_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/name.basics.tsv.gz")

# Load to Iceberg
name_basics_df.writeTo("demo.bronze.name_basics").createOrReplace()

spark.stop()