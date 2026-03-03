 WITH distinct_title_types AS (
    SELECT
        DISTINCT 
        titleType AS title_type_name,
        CAST(snapshot_date AS DATE) AS snapshot_date,
        CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp   
    FROM {{ source('stage_bronze', 'title_basics') }}
    ORDER BY title_type_name ASC
)

SELECT
    UUID() AS title_type_id,
    title_type_name,
    snapshot_date,
    ingested_at_timestamp
FROM distinct_title_types
