WITH unique_attribute AS (
SELECT  
  DISTINCT (
    CASE
      WHEN attributes = '\\N' THEN NULL
      ELSE attributes
    END) AS title_context_name,
    CAST(snapshot_date AS DATE),
    CAST(ingested_at_timestamp AS TIMESTAMP),
    snapshot_try
FROM {{ source('stage_raw', 'title_akas') }}
)
SELECT
  {{ dbt_utils.generate_surrogate_key(['title_context_name']) }} AS title_context_id,
  title_context_name,
  snapshot_date,
  ingested_at_timestamp,
  snapshot_try
FROM unique_attribute
WHERE title_context_name IS NOT NULL