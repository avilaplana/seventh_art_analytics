{{ config(
    materialized='incremental',
    unique_key='person_id'
) }}

WITH latest_snapshot_name_basics AS (
    SELECT *
    FROM {{ source('bronze', 'name_basics') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'name_basics') }}
    )
)

SELECT 
  nconst AS person_id,
  primaryName AS name,
  CASE 
    WHEN birthYear = '\\N' THEN NULL
    ELSE CAST(birthYear AS SMALLINT)
  END AS birth_year,
  CASE 
    WHEN deathYear = '\\N' THEN NULL
    ELSE CAST(deathYear AS SMALLINT)
  END AS death_year
FROM latest_snapshot_name_basics
