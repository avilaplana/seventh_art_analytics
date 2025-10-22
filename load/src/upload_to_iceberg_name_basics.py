from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("load-name-basics-Iceberg-MinIO") \
    .getOrCreate()

# Drop table demo.imdb.name_basics
spark.sql("""DROP TABLE IF EXISTS demo.imdb.name_basics""")

# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.name_basics (
    id STRING,
    primary_name STRING,
    birth_year STRING,
    death_year STRING,
    primary_profession STRING,
    titles STRING
) USING iceberg
""")



# Read from raw data from S3
name_basics_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv(f"s3a://{object_path}/name.basics.tsv.gz")

# Rename columns
name_rename_columns_df = name_basics_df .withColumnsRenamed({"nconst": "id", "primaryName": "primary_name", "birthYear": "birth_year", "deathYear": "death_year", "primaryProfession": "primary_profession", "knownForTitles": "titles"})

# Set NULL when \N is found
name_basics_set_null_death_year_df = name_rename_columns_df.withColumn("death_year",when(col("death_year") == "\\N", None).otherwise(col("death_year")))

# Load to Iceberg
name_basics_set_null_death_year_df.writeTo("demo.imdb.name_basics").createOrReplace()

spark.stop()