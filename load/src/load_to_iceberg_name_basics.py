from pyspark.sql import SparkSession
from s3_utils import object_path
from pyspark.sql.functions import current_date

spark = SparkSession.builder \
    .appName("load-name-basics-Iceberg-MinIO") \
    .getOrCreate()

# Read from raw data from S3/MinIO
name_basics_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/name.basics.tsv.gz")

# Load to Iceberg
name_basics_df \
    .withColumn("ingestion_date", current_date()) \
    .writeTo("demo.bronze.name_basics") \
    .overwritePartitions()

spark.stop()