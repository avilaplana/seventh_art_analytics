SELECT 
  nconst AS person_id,
  primaryName AS name,
  CASE 
    WHEN birthYear = '\\N' THEN NULL
    ELSE CAST(birthYear AS INT)
  END AS birth_year,
  CASE 
    WHEN deathYear = '\\N' THEN NULL
    ELSE CAST(deathYear AS INT)
  END AS death_year
FROM {{ source('bronze', 'name_basics') }}
