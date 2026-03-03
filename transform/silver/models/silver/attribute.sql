WITH unique_attribute AS (
SELECT  
  DISTINCT (
    CASE
      WHEN attributes = '\\N' THEN NULL
      ELSE attributes
    END) AS attribute_name,
    CAST(snapshot_date AS DATE),
    CAST(ingested_at_timestamp AS TIMESTAMP),
    snapshot_try
FROM {{ source('stage_bronze', 'title_akas') }}
)
SELECT
  uuid() AS attribute_id,
  attribute_name,
  snapshot_date,
  ingested_at_timestamp,
  snapshot_try
FROM unique_attribute
WHERE attribute_name IS NOT NULL