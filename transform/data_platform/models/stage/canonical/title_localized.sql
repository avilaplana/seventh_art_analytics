WITH title_akas_cleaned AS (
  SELECT
    titleId AS title_id, 
    title AS title,
    CAST(ordering AS INT) AS ordering,
    CASE
        WHEN region = '\\N' THEN NULL
        ELSE region
      END AS region_code,
    CASE
        WHEN language = '\\N' THEN NULL
        ELSE language
      END AS language_code,
    CASE
        WHEN attributes = '\\N' THEN NULL
        ELSE attributes
      END AS title_context_name,  
    CASE
      WHEN isOriginalTitle = 0 THEN FALSE
      ELSE TRUE
    END AS is_original_title,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    snapshot_try
  FROM {{ source('stage_raw', 'title_akas') }}
)

SELECT 
  tac.title_id,
  tac.title,
  tac.ordering,
  rr.region_id,
  ll.language_id,
  tc.title_context_id,
  tac.is_original_title,
  tac.snapshot_date,
  tac.ingested_at_timestamp,
  tac.snapshot_try
FROM title_akas_cleaned tac
LEFT JOIN {{ ref('regions') }} rr
ON tac.region_code = rr.region_code
LEFT JOIN {{ ref('languages') }} ll
ON tac.language_code = ll.language_code
LEFT JOIN {{ ref('title_context') }} tc
ON tac.title_context_name = tc.title_context_name