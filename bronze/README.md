# Bronze Layer: IMDB Data Ingestion to S3/MinIO

This folder contains a small pipeline to **ingest IMDB raw datasets** and upload them to a **local S3-compatible object store** (MinIO). This represents the **bronze layer** in a typical data lake architecture.

---

## Project Structure

```
.
├── docker-compose.yml # MinIO service definition
├── Makefile # Commands to run MinIO and test locally
├── raw_data # (ignored) local storage for downloaded files
├── requirements.txt # Python dependencies
└── upload_to_s3.py # Python script to stream IMDB data to S3
```

---

## IMDB Raw Data

The following IMDB datasets are ingested:

- `name.basics.tsv.gz`
- `title.akas.tsv.gz`
- `title.basics.tsv.gz`
- `title.crew.tsv.gz`
- `title.episode.tsv.gz`
- `title.principals.tsv.gz`
- `title.ratings.tsv.gz`

These files are downloaded from `https://datasets.imdbws.com/` and uploaded directly to MinIO/S3 **without storing them permanently on disk** (streamed upload).

---

## Environment Variables

The script uses environment variables for S3 credentials:

| Variable      | Default (for local MinIO)            | Description                          |
|---------------|-------------------------------------|--------------------------------------|
| `S3_ENDPOINT` | `http://localhost:9000`             | MinIO/S3 endpoint                     |
| `ACCESS_KEY`  | `minioadmin`                        | MinIO access key                       |
| `SECRET_KEY`  | `minioadmin123`                     | MinIO secret key                       |
| `REGION`      | `eu-west-2`                         | S3 region name                         |
| `ROOT_BUCKET` | `data` (set in the script)          | Target bucket for uploaded files      |

---

## How to Run Locally

1. **Start MinIO**

```bash
make up
```

This will start a MinIO container with the console available on port 9001.

2. Install Python dependencies and run the ingestion

```bash
make test
```

This will:

- Create or use a local Python virtual environment
- Install dependencies from requirements.txt
- Stream IMDB datasets to the S3 bucket defined in ROOT_BUCKET

3. Stop and clean MinIO

```bash
make clean
```

## File Storage in S3

Files are uploaded to S3 using **date-based partitioning**:

```bash
s3://data/imdb/year=YYYY/month=MM/day=DD/<filename>.tsv.gz
```

- Each day’s ingestion gets its own folder for easier management.  
- This structure is compatible with tools like Spark, Hive, or Athena.

