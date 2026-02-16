WITH unique_attribute AS (
SELECT  
  DISTINCT (
    CASE
      WHEN attributes = '\\N' THEN NULL
      ELSE attributes
    END) AS attribute_name
FROM {{ source('bronze', 'title_akas') }}
)
SELECT
  uuid() AS attribute_id,
  attribute_name
FROM unique_attribute
WHERE attribute_name IS NOT NULL