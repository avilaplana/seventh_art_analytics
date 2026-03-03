from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("create-tables-Iceberg-MinIO") \
    .getOrCreate()

# Create Iceberg Tables
spark.sql("""CREATE NAMESPACE IF NOT EXISTS default""")

# Drop tables
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.name_basics""")
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.title_akas""")
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.title_basics""")
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.title_crew""")
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.title_episode""")
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.title_principals""")
spark.sql("""DROP TABLE IF EXISTS demo.stage_bronze.title_ratings""")

# Create Iceberg Tables
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.name_basics (
    nconst STRING,
    primaryName STRING,
    birthYear STRING,
    deathYear STRING,
    primaryProfession STRING,
    knownForTitles STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.title_akas (
    titleId STRING,
    ordering STRING,
    title STRING,
    region STRING,
    language STRING,
    types STRING,
    attributes STRING,
    isOriginalTitle STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.title_basics (
    tconst STRING,
    titleType STRING,
    primaryTitle STRING,
    originalTitle STRING,
    isAdult STRING,
    startYear STRING,
    endYear STRING,
    runtimeMinutes STRING,
    genres STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.title_crew (
    tconst STRING,
    directors STRING,
    writers STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.title_episode (
    tconst STRING,
    parentTconst STRING,
    seasonNumber STRING,
    episodeNumber STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.title_principals (
    tconst STRING,
    ordering STRING,
    nconst STRING,
    category STRING,
    job STRING,
    characters STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.stage_bronze.title_ratings (
    tconst STRING,
    averageRating STRING,
    numVotes STRING,
    snapshot_date DATE,
    ingested_at_timestamp TIMESTAMP,
    snapshot_try INT
) USING iceberg
""")

spark.stop()