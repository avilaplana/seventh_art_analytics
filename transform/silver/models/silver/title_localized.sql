WITH title_akas_cleaned AS (
  SELECT
    titleId as title_id, 
    title as title,
    ordering as ordering,
    CASE
        WHEN region = '\\N' OR region is NULL THEN 'UNKNOWN'
        ELSE region
      END AS region_name,
    CASE
        WHEN language = '\\N' OR language is NULL THEN 'UNKNOWN'
        ELSE language
      END AS language_name, 
    CASE
        WHEN attributes = '\\N' OR attributes is NULL THEN 'UNKNOWN'
        ELSE attributes
      END AS attribute_name,  
    CASE
      WHEN isOriginalTitle = 0 THEN FALSE
      ELSE TRUE
    END AS is_original_title  
  FROM {{ source('bronze', 'title_akas') }}
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
LEFT JOIN {{ ref('region') }} rr
ON tac.region_name = rr.region_name
LEFT JOIN {{ ref('language') }} ll
ON tac.language_name = ll.language_name
LEFT JOIN {{ ref('attribute') }} aa
ON tac.attribute_name = aa.attribute_name