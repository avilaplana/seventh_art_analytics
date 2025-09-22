import boto3
from botocore.client import Config
from datetime import datetime
import os
import requests

S3_ENDPOINT = os.environ['S3_ENDPOINT']
ACCESS_KEY = os.environ['ACCESS_KEY']
SECRET_KEY = os.environ['SECRET_KEY']
REGION = os.environ['REGION']
SOURCE_URL_PREFIX = "https://datasets.imdbws.com/"
ROOT_BUCKET = "data"

RAW_FILES = [
    "name.basics.tsv.gz",
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
]

def download_raw_data_from_source(url):
    """
    Return a file-like object streaming from the URL
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    return response.raw  # raw is a file-like object


def upload_raw_data_to_s3(s3_client, file_obj, bucket: str, raw_file: str):
    """
    Upload a file-like object to S3/MinIO with date partitioning.

    Parameters:
    - s3_client: boto3 S3 client
    - file_obj: file-like object (streaming)
    - bucket: target bucket name
    - raw_file: name of the file (e.g., 'title.basics.tsv.gz')
    """
    # Get current UTC date
    today = datetime.utcnow()
    year = today.year
    month = f"{today.month:02d}"
    day = f"{today.day:02d}"

    # Build S3 object key with partition
    object_key = f"imdb/year={year}/month={month}/day={day}/{raw_file}"

    # Upload the file-like object to S3/MinIO
    s3_client.upload_fileobj(file_obj, bucket, object_key)

    print(f"Uploaded {raw_file} → {bucket}/{object_key}")

def main():
    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=REGION,
    )

    # Ensure bucket exists
    try:
        s3.head_bucket(Bucket=ROOT_BUCKET)
        print(f"Bucket {ROOT_BUCKET} already exists")
    except:
        s3.create_bucket(Bucket=ROOT_BUCKET)
        print(f"Created bucket {ROOT_BUCKET}")

    for raw_file in RAW_FILES:
        url = f"{SOURCE_URL_PREFIX}{raw_file}"
        print(f"Streaming {raw_file} from {url}")
        file_obj = download_raw_data_from_source(url)
        upload_raw_data_to_s3(s3, file_obj, ROOT_BUCKET, raw_file)

if __name__ == "__main__":
    main()