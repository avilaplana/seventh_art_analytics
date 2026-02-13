{{ config(
    materialized='incremental',
    unique_key='genre_id'
) }}

WITH latest_snapshot_title_basics AS (
    SELECT *
    FROM {{ source('bronze', 'title_basics') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_basics') }}
    )
),
array_genres AS (
    SELECT SPLIT(TRIM(genres), ',') AS genres
    FROM latest_snapshot_title_basics
), 
distinct_genres AS (
    SELECT
        DISTINCT(
            CASE
                WHEN genre = '\\N' OR genre is NULL THEN 'UNKNOWN'
                ELSE genre
            END) AS genre_name
    FROM array_genres
    LATERAL VIEW explode(genres) AS genre
    ORDER BY genre_name ASC
)

SELECT
    UUID() AS genre_id,
    genre_name
FROM distinct_genres