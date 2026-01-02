{{ config(materialized='table', file_format='iceberg') }}


SELECT 
  nconst AS person_id,
  primaryName AS prumary_name,
  CASE 
    WHEN birthYear = '\\N' THEN NULL
    ELSE CAST(birthYear AS SMALLINT)
  END AS birth_year,
  CASE 
    WHEN deathYear = '\\N' THEN NULL
    ELSE CAST(deathYear AS SMALLINT)
  END AS death_year
FROM {{ source('bronze', 'name_basics') }}
