{{ config(
    materialized='incremental',
    unique_key='title_type_id'
) }}

WITH latest_snapshot_title_basics AS (
    SELECT *
    FROM {{ source('bronze', 'title_basics') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_basics') }}
    )
),
 distinct_title_types AS (
    SELECT
        DISTINCT titleType AS title_type_name
    FROM latest_snapshot_title_basics
    ORDER BY title_type_name ASC
)

SELECT
    UUID() AS title_type_id,
    title_type_name
FROM distinct_title_types
