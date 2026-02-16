WITH array_genres AS (
    SELECT SPLIT(TRIM(genres), ',') AS genres,
    tconst as title_id
    FROM {{ source('bronze', 'title_basics') }}
), 
title_genre AS (
    SELECT
        title_id,
        CASE
            WHEN genre = '\\N' THEN NULL
            ELSE genre
        END AS genre_name
    FROM array_genres
    LATERAL VIEW explode(genres) AS genre
    ORDER BY title_id, genre_name ASC
)

SELECT
    tg.title_id,
    g.genre_id
FROM title_genre tg
LEFT JOIN  {{ ref('genre') }} g
ON tg.genre_name = g.genre_name
WHERE tg.genre_name IS NOT NULL