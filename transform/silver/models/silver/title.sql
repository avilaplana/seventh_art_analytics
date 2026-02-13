{{ config(
    materialized='incremental',
    unique_key='title_id'
) }}

WITH latest_snapshot_title_basics AS (
    SELECT *
    FROM {{ source('bronze', 'title_basics') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_basics') }}
    )
)
,latest_snapshot_title_ratings AS (
    SELECT *
    FROM {{ source('bronze', 'title_ratings') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_ratings') }}
    )
)

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
    END AS duration_minutes,
        COALESCE(tr.averageRating, 0) AS average_rating,
        COALESCE(tr.numVotes, 0) AS number_of_votes
FROM latest_snapshot_title_basics tb
LEFT JOIN {{ ref('title_type') }} tt
ON tb.titleType = tt.title_type_name
LEFT JOIN latest_snapshot_title_ratings tr
ON tb.tconst = tr.tconst
