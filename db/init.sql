-- Airflow database and user
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;

-- REST-Iceberg catalog database and user
CREATE USER iceberg WITH PASSWORD 'iceberg';
CREATE DATABASE catalog_metadata OWNER iceberg;

-- Optional: grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
GRANT ALL PRIVILEGES ON DATABASE catalog_metadata TO iceberg;
