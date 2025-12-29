from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-title-episode-ratings-MinIO") \
    .getOrCreate()

# Drop ALL the tables
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_ratings""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_ratings (
    tconst STRING,
    averageRating STRING,
    numVotes STRING
) USING iceberg
""")

# Read from raw data from S3
ratings_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/title.ratings.tsv.gz")

# Load to Iceberg
ratings_df.writeTo("demo.bronze.title_ratings").createOrReplace()

spark.stop()