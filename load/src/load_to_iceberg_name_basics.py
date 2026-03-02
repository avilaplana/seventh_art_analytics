import argparse
from s3_utils import object_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

def main(snapshot_date: str, ingested_at_timestamp: str):

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
        .withColumn("snapshot_date", lit(snapshot_date)) \
        .withColumn("ingested_at_timestamp", lit(ingested_at_timestamp)) \
        .writeTo("demo.stage_bronze.name_basics").createOrReplace()

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load name_basics to Iceberg")
    parser.add_argument("--snapshot_date", required=True, help="Snapshot date YYYY-MM-DD")
    parser.add_argument("--ingested_at_timestamp", required=True, help="Snapshot timestamp UTC YYYY-MM-DD HH:MM:SS")
    
    args = parser.parse_args()

    # Call main function with the parsed arguments
    main(args.snapshot_date, args.ingested_at_timestamp)