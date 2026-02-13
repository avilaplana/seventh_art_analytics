{{ config(
    materialized='incremental',
    unique_key='attribute_id'
) }}

WITH latest_snapshot_title_akas AS (
    SELECT *
    FROM {{ source('bronze', 'title_akas') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_akas') }}
    )
),
unique_attribute AS (
SELECT  
  DISTINCT (
    CASE
      WHEN attributes = '\\N' OR attributes is NULL THEN 'UNKNOWN'
      ELSE attributes
    END) AS attribute_name
FROM  latest_snapshot_title_akas
)
SELECT uuid() AS attribute_id, attribute_name FROM unique_attribute