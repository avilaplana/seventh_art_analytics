from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-episode-ratings-MinIO") \
    .getOrCreate()

# Drop ALL the tables
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_ratings""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_ratings (
    title_id STRING,
    average_rating STRING,
    num_votes STRING
) USING iceberg
""")

# Read from raw data from S3
ratings_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.ratings.tsv.gz")

# Rename columns
ratings_rename_columns_df = ratings_df.withColumnsRenamed({"tconst": "title_id", "averageRating": "average_rating", "numVotes": "num_votes"})

# Set NULL when \N is found
ratings_set_null_values_df = ratings_rename_columns_df \
    .withColumn("average_rating",when(col("average_rating") == "\\N", None).otherwise(col("average_rating"))) \
    .withColumn("num_votes",when(col("num_votes") == "\\N", None).otherwise(col("num_votes")))

# Load to Iceberg
ratings_set_null_values_df.writeTo("demo.imdb.title_ratings").createOrReplace()

