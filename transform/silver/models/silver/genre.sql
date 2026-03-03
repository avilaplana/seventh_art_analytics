WITH array_genres AS (
    SELECT SPLIT(TRIM(genres), ',') AS genres,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    snapshot_try
    FROM {{ source('stage_bronze', 'title_basics') }}
), 

distinct_genres AS (
    SELECT
        DISTINCT
            CASE
                WHEN genre = '\\N' THEN NULL
                ELSE genre
            END AS genre_name,
            snapshot_date,
            ingested_at_timestamp,
            snapshot_try
    FROM array_genres
    LATERAL VIEW explode(genres) AS genre
    ORDER BY genre_name ASC
)

SELECT
    UUID() AS genre_id,
    genre_name,
    snapshot_date,
    ingested_at_timestamp,
    snapshot_try
FROM distinct_genres
WHERE genre_name IS NOT NULL