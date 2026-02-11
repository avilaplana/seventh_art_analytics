{% test distinct_region_count_matches(model) %}

WITH source_counts AS (
    SELECT COUNT(DISTINCT region) AS cnt
    FROM {{ source('bronze', 'title_akas') }}
    WHERE region IS NOT NULL
      AND region != '\\N'
),
model_counts AS (
    SELECT COUNT(DISTINCT r.region_code) AS cnt
    FROM {{ model }} tl
    INNER JOIN {{ ref('regions') }} r
        ON tl.region_id = r.region_id
)
SELECT *
FROM source_counts, model_counts
WHERE source_counts.cnt != model_counts.cnt

{% endtest %}