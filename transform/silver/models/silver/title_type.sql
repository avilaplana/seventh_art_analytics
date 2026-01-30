 WITH distinct_title_types AS (
    SELECT
        DISTINCT titleType AS title_type_name
    FROM {{ source('bronze', 'title_basics') }}
    ORDER BY title_type_name ASC
)

SELECT
    UUID() AS title_type_id,
    title_type_name
FROM distinct_title_types
