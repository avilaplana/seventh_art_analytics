WITH unique_language AS (
SELECT  
  DISTINCT (
    CASE
      WHEN language = '\\N' OR language IS NULL THEN 'UNKNOWN'
      ELSE language
    END) AS language_name
FROM {{ source('bronze', 'title_akas') }}
)
SELECT uuid() AS language_id, language_name FROM unique_language