from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("create-tables-Iceberg-MinIO") \
    .getOrCreate()

# Drop tables
spark.sql("""DROP TABLE IF EXISTS demo.bronze.name_basics""")
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_akas""")
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_basics""")
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_crew""")
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_episode""")
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_principals""")
spark.sql("""DROP TABLE IF EXISTS demo.bronze.title_ratings""")

# Create Iceberg Tables
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

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_akas (
    titleId STRING,
    ordering STRING,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    isOriginalTitle STRING
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_basics (
    tconst STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult STRING,
    startYear STRING,
    endYear STRING,
    runtimeMinutes STRING,
    genres STRING
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_crew (
    tconst STRING,
    directors STRING,
    writers STRING
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_episode (
    tconst STRING,
    parentTconst STRING,
    seasonNumber STRING,
    episodeNumber STRING
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_principals (
    tconst STRING,
    ordering STRING,
    nconst STRING,
    category STRING,
    job STRING,
    characters STRING
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_ratings (
    tconst STRING,
    averageRating STRING,
    numVotes STRING
) USING iceberg
""")

spark.stop()