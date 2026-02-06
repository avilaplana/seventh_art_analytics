
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
FROM {{ source('bronze', 'title_basics') }} tb
LEFT JOIN {{ ref('title_type') }} tt
ON tb.titleType = tt.title_type_name
LEFT JOIN {{ source('bronze', 'title_ratings') }} tr
ON tb.tconst = tr.tconst
