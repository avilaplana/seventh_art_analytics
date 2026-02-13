from pyspark.sql import SparkSession
from s3_utils import object_path

spark = SparkSession.builder \
    .appName("create-tables-Iceberg-MinIO") \
    .getOrCreate()

# Create Iceberg Tables
spark.sql("""CREATE NAMESPACE IF NOT EXISTS default""")

# Create Iceberg Tables
spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.name_basics (
    nconst STRING,
    primaryName STRING,
    birthYear STRING,
    deathYear STRING,
    primaryProfession STRING,
    knownForTitles STRING,
    ingestion_date DATE 
)
USING iceberg
PARTITIONED BY (ingestion_date)
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
    isOriginalTitle STRING,
    ingestion_date DATE
)
USING iceberg
PARTITIONED BY (ingestion_date)    
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
    genres STRING,
    ingestion_date DATE
)
USING iceberg
PARTITIONED BY (ingestion_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_crew (
    tconst STRING,
    directors STRING,
    writers STRING,
    ingestion_date DATE
)
USING iceberg
PARTITIONED BY (ingestion_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_episode (
    tconst STRING,
    parentTconst STRING,
    seasonNumber STRING,
    episodeNumber STRING,
    ingestion_date DATE
)
USING iceberg
PARTITIONED BY (ingestion_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_principals (
    tconst STRING,
    ordering STRING,
    nconst STRING,
    category STRING,
    job STRING,
    characters STRING,
    ingestion_date DATE          
) 
USING iceberg
PARTITIONED BY (ingestion_date)
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS demo.bronze.title_ratings (
    tconst STRING,
    averageRating STRING,
    numVotes STRING,
    ingestion_date DATE
)
USING iceberg
PARTITIONED BY (ingestion_date)
""")

spark.stop()