WITH unique_attribute AS (
SELECT  
  DISTINCT (
    CASE
      WHEN attributes = '\\N' OR attributes is NULL THEN 'UNKNOWN'
      ELSE attributes
    END) AS attribute_name
FROM {{ source('bronze', 'title_akas') }}
)
SELECT uuid() AS attribute_id, attribute_name FROM unique_attribute