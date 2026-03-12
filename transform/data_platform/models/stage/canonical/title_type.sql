 WITH distinct_title_types AS (
    SELECT
        DISTINCT 
        titleType AS title_type_name,
        CAST(snapshot_date AS DATE) AS snapshot_date,
        CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
        snapshot_try
    FROM {{ source('stage_raw', 'title_basics') }}
    ORDER BY title_type_name ASC
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['title_type_name']) }} AS title_type_id,
    title_type_name,
    snapshot_date,
    ingested_at_timestamp,
    snapshot_try
FROM distinct_title_types
