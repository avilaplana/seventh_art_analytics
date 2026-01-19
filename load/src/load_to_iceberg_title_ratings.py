from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-episode-ratings-MinIO") \
    .getOrCreate()

# Read from raw data from S3
ratings_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.ratings.tsv.gz")

# Load to Iceberg
ratings_df.writeTo("demo.bronze.title_ratings").createOrReplace()

spark.stop()