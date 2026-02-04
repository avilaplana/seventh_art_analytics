WITH unique_region AS (
SELECT  
  DISTINCT (
    CASE
      WHEN region = '\\N' OR region is NULL THEN 'UNKNOWN'
      ELSE region
    END) AS region_name
FROM {{ source('bronze', 'title_akas') }}
)
SELECT uuid() AS region_id, region_name FROM unique_region