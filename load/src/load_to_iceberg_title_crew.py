import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from s3_utils import object_path

def main(snapshot_date: str, ingested_at_timestamp: str):

    spark = SparkSession.builder \
        .appName("load-title-crew-Iceberg-MinIO") \
        .getOrCreate()

    # Read from raw data from S3
    title_crew_df = spark.read \
        .option("header", True) \
        .option("sep", "\t") \
        .csv(f"s3a://{object_path}/title.crew.tsv.gz")

    # Load to Iceberg
    title_crew_df \
        .withColumn("snapshot_date", lit(snapshot_date)) \
        .withColumn("ingested_at_timestamp", lit(ingested_at_timestamp)) \
        .writeTo("demo.stage_bronze.title_crew").createOrReplace()

    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load title_crew to Iceberg")
    parser.add_argument("--snapshot_date", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--ingested_at_timestamp", required=True, help="Snapshot timestamp UTC YYYY-MM-DD HH:MM:SS")
    
    args = parser.parse_args()

    # Call main function with the parsed arguments
    main(args.snapshot_date, args.ingested_at_timestamp)