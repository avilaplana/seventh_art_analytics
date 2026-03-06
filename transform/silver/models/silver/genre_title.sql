WITH array_genres AS (
    SELECT SPLIT(TRIM(genres), ',') AS genres,
    tconst as title_id,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    snapshot_try
    FROM {{ source('stage_raw', 'title_basics') }}
), 
title_genre AS (
    SELECT
        title_id,
        CASE
            WHEN genre = '\\N' THEN NULL
            ELSE genre
        END AS genre_name,
        snapshot_date,
        ingested_at_timestamp,
        snapshot_try      
    FROM array_genres
    LATERAL VIEW explode(genres) AS genre
    ORDER BY title_id, genre_name ASC
)

SELECT
    tg.title_id,
    g.genre_id,
    tg.snapshot_date,
    tg.ingested_at_timestamp,
    tg.snapshot_try      
FROM title_genre tg
LEFT JOIN  {{ ref('genre') }} g
ON tg.genre_name = g.genre_name
WHERE tg.genre_name IS NOT NULL