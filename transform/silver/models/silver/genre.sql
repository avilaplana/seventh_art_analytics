WITH array_genres AS (
    SELECT SPLIT(TRIM(genres), ',') AS genres
    FROM {{ source('bronze', 'title_basics') }}
), distinct_genres AS (
SELECT
    DISTINCT genre AS genre_name
FROM array_genres
LATERAL VIEW explode(genres) AS genre
WHERE genre != '\\N'
ORDER BY genre_name ASC
)
SELECT UUID() AS genre_id, genre_name FROM distinct_genres