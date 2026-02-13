{{ config(
    materialized='incremental',
    unique_key=['title_id', 'ordering', 'region_id', 'language_id', 'attribute_id']
) }}

WITH latest_snapshot_title_akas AS (
    SELECT *
    FROM {{ source('bronze', 'title_akas') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_akas') }}
    )
),
title_akas_cleaned AS (
  SELECT
    titleId as title_id, 
    title as title,
    ordering as ordering,
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
      END AS attribute_name,  
    CASE
      WHEN isOriginalTitle = 0 THEN FALSE
      ELSE TRUE
    END AS is_original_title  
  FROM latest_snapshot_title_akas
)

SELECT 
  tac.title_id,
  tac.title,
  tac.ordering,
  rr.region_id,
  ll.language_id,
  aa.attribute_id,
  tac.is_original_title
FROM title_akas_cleaned tac
LEFT JOIN {{ ref('regions') }} rr
ON tac.region_code = rr.region_code
LEFT JOIN {{ ref('languages') }} ll
ON tac.language_code = ll.language_code
LEFT JOIN {{ ref('attribute') }} aa
ON tac.attribute_name = aa.attribute_name