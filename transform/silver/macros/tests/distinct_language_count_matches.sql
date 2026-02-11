{% test distinct_language_count_matches(model) %}

WITH source_counts AS (
    SELECT COUNT(DISTINCT language) AS cnt
    FROM {{ source('bronze', 'title_akas') }}
    WHERE language IS NOT NULL
      AND language != '\\N'
),
model_counts AS (
    SELECT COUNT(DISTINCT r.language_code) AS cnt
    FROM {{ model }} tl
    INNER JOIN {{ ref('languages') }} r
        ON tl.language_id = r.language_id
)
SELECT *
FROM source_counts, model_counts
WHERE source_counts.cnt != model_counts.cnt

{% endtest %}