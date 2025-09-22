from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
    .appName("Iceberg-MinIO") \
    .getOrCreate()

# Drop ALL the tables
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_basics""")
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_akas""")
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_crew""")
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_episode""")
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_ratings""")
spark.sql("""DROP TABLE IF EXISTS demo.imdb.title_principals""")
spark.sql("""DROP TABLE IF EXISTS demo.imdb.name_basics""")

# title_basics
# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_basics (
    title_id STRING,
    title_type STRING,
    primary_title STRING,
    original_title STRING,
    is_adult STRING,
    start_year STRING,
    end_year STRING,
    runtime_minutes STRING,
    genres STRING
) USING iceberg
""")

# Read from raw data from S3
title_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv("s3a://data/imdb/year=2025/month=09/day=22/title.basics.tsv.gz")


# Rename columns# Rename columns
title_rename_columns_df = title_df.withColumnsRenamed({"tconst": "title_id", "titleType": "title_type", "primaryTitle": "primary_title", "originalTitle": "original_title", "isAdult": "is_adult", "startYear": "start_year", "endYear": "end_year", "runtimeMinutes": "runtime_minutes"})

# Set NULL when \N is found
title_basics_set_null_values_df = title_rename_columns_df \
    .withColumn("end_year",when(col("end_year") == "\\N", None).otherwise(col("end_year")))


# Load to Iceberg# Load to Iceberg
title_basics_set_null_values_df.writeTo("demo.imdb.title_basics").createOrReplace()

# title_akas
# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_akas (
    title_id STRING,
    ordering STRING,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    is_original_title STRING
) USING iceberg
""")

# Read from raw data from S3
title_akas_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv("s3a://data/imdb/year=2025/month=09/day=22/title.akas.tsv.gz")

# Rename columns
title_akas_rename_columns_df = title_akas_df.withColumnsRenamed({"titleId": "title_id", "isOriginalTitle": "is_original_title"})

# Set NULL when \N is found
title_akas_set_null_values_df = title_akas_rename_columns_df \
    .withColumn("region",when(col("region") == "\\N", None).otherwise(col("region"))) \
    .withColumn("language",when(col("language") == "\\N", None).otherwise(col("language"))) \
    .withColumn("types",when(col("types") == "\\N", None).otherwise(col("types"))) \
    .withColumn("attributes",when(col("attributes") == "\\N", None).otherwise(col("attributes")))

# Load to Iceberg
title_akas_set_null_values_df.writeTo("demo.imdb.title_akas").createOrReplace()

# title_crew
# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_crew (
    title_id STRING,
    director_id STRING,
    writers STRING
) USING iceberg
""")

# Read from raw data from S3
title_crew_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv("s3a://data/imdb/year=2025/month=09/day=22/title.crew.tsv.gz")

# Rename columns
title_crew_rename_columns_df = title_crew_df.withColumnsRenamed({"tconst": "title_id", "directors": "director_id"})

# Set NULL when \N is found
title_crew_set_null_values_df = title_crew_rename_columns_df \
    .withColumn("writers",when(col("writers") == "\\N", None).otherwise(col("writers")))

# Load to Iceberg
title_crew_set_null_values_df.writeTo("demo.imdb.title_crew").createOrReplace()

# title_episode
# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_episode (
    title_id STRING,
    parent_title_id STRING,
    season_number STRING,
    episode_number STRING
) USING iceberg
""")

# Read from raw data from S3
title_episode_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv("s3a://data/imdb/year=2025/month=09/day=22/title.episode.tsv.gz")

# Rename columns
title_episode_rename_columns_df = title_episode_df.withColumnsRenamed({"tconst": "title_id", "parentTconst": "parent_title_id", "seasonNumber": "season_number", "episodeNumber": "episode_number"})

# Set NULL when \N is found
title_episode_set_null_values_df = title_episode_rename_columns_df \
    .withColumn("season_number",when(col("season_number") == "\\N", None).otherwise(col("season_number"))) \
    .withColumn("episode_number",when(col("episode_number") == "\\N", None).otherwise(col("episode_number")))

# Load to Iceberg
title_episode_set_null_values_df.writeTo("demo.imdb.title_episode").createOrReplace()


# title_principals
# Create Iceberg Table
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.imdb.title_principals (
    title_id STRING,
    ordering STRING,
    name_id STRING,
    category STRING,
    job STRING,
    characters STRING
) USING iceberg
""")

# Read from raw data from S3
title_principals_df = spark.read \
    .option("header", True) \
    .option("sep", "\t") \
    .csv("s3a://data/imdb/year=2025/month=09/day=22/title.principals.tsv.gz")

# Rename columns
title_principals_rename_columns_df = title_principals_df.withColumnsRenamed({"tconst": "title_id", "nconst": "name_id"})

# Set NULL when \N is found
title_principals_set_null_values_df = title_principals_rename_columns_df \
    .withColumn("job",when(col("job") == "\\N", None).otherwise(col("job"))) \
    .withColumn("characters",when(col("characters") == "\\N", None).otherwise(col("characters")))

# Load to Iceberg
title_principals_set_null_values_df.writeTo("demo.imdb.title_principals").createOrReplace()

# title_ratings
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
    .csv("s3a://data/imdb/year=2025/month=09/day=22/title.ratings.tsv.gz")

# Rename columns
ratings_rename_columns_df = ratings_df.withColumnsRenamed({"tconst": "title_id", "averageRating": "average_rating", "numVotes": "num_votes"})

# Set NULL when \N is found
ratings_set_null_values_df = ratings_rename_columns_df \
    .withColumn("average_rating",when(col("average_rating") == "\\N", None).otherwise(col("average_rating"))) \
    .withColumn("num_votes",when(col("num_votes") == "\\N", None).otherwise(col("num_votes")))

# Load to Iceberg
ratings_set_null_values_df.writeTo("demo.imdb.title_ratings").createOrReplace()

# name_basics
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
    .csv("s3a://data/imdb/year=2025/month=09/day=22/name.basics.tsv.gz")

# Rename columns
name_rename_columns_df = name_basics_df .withColumnsRenamed({"nconst": "id", "primaryName": "primary_name", "birthYear": "birth_year", "deathYear": "death_year", "primaryProfession": "primary_profession", "knownForTitles": "titles"})

# Set NULL when \N is found
name_basics_set_null_death_year_df = name_rename_columns_df.withColumn("death_year",when(col("death_year") == "\\N", None).otherwise(col("death_year")))

# Load to Iceberg
name_basics_set_null_death_year_df.writeTo("demo.imdb.name_basics").createOrReplace()

