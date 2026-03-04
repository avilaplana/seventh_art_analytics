
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
        CAST(COALESCE(tr.averageRating, 0) AS DOUBLE) AS average_rating,
        CAST(COALESCE(tr.numVotes, 0) AS INT) AS number_of_votes,
    CAST(tb.snapshot_date AS DATE) AS snapshot_date,
    CAST(tb.ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    tb.snapshot_try
FROM {{ source('stage_bronze', 'title_basics') }} tb
LEFT JOIN {{ ref('title_type') }} tt
ON tb.titleType = tt.title_type_name
LEFT JOIN {{ source('stage_bronze', 'title_ratings') }} tr
ON tb.tconst = tr.tconst
