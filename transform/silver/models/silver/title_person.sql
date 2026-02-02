SELECT 
tconst AS title_id,
nconst AS person_id,
ordering, 
CASE
     WHEN characters = '\\N' THEN NULL
    ELSE SPLIT(TRIM(REPLACE(REPLACE(REPLACE(characters, '[', ''), ']', ''), '\"','')), ',')
END AS characters
FROM {{ source('bronze', 'title_principals') }}