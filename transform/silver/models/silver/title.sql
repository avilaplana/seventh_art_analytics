
SELECT
    tb.tconst AS title_id,
    tt.title_type_id AS title_type_id,
    tb.primaryTitle AS primary_title,
    tb.originalTitle AS original_title,
CASE 
    WHEN tb.isAdult = '1' THEN TRUE 
    ELSE FALSE 
END AS is_adult,
CASE
    WHEN tb.startYear = '\\N' THEN NULL
    ELSE CAST(tb.startYear AS INT)
END AS release_year,
CASE
  WHEN tb.runtimeMinutes = '\\N' THEN NULL
  ELSE CAST(tb.runtimeMinutes AS INT)
END AS duration_minutes
FROM {{ source('bronze', 'title_basics') }} tb
INNER JOIN {{ ref('title_type') }} tt
ON tb.titleType = tt.title_type_name