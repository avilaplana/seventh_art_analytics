WITH unique_attributes AS (
SELECT  
  DISTINCT (
    CASE
      WHEN attributes = '\\N' THEN NULL
      ELSE attributes
    END) AS title_context_name,
    CAST(snapshot_date AS DATE),
    CAST(ingested_at_timestamp AS TIMESTAMP),
    snapshot_try
FROM {{ source('stage_raw', 'title_akas') }}
),
classified AS (
    SELECT
        u.title_context_name,
        m.variant_category,
        ROW_NUMBER() OVER (
            PARTITION BY u.title_context_name
            ORDER BY m.priority
        ) AS rn,
        u.snapshot_date,
        u.ingested_at_timestamp,
        u.snapshot_try
    FROM unique_attributes u
    LEFT JOIN {{ ref('title_variant_pattern_map') }} m
        ON LOWER(u.title_context_name) LIKE m.pattern
    WHERE u.title_context_name IS NOT NULL   
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['title_context_name']) }} AS title_context_id,
    title_context_name,
    COALESCE(variant_category, 'other') AS title_context_category,
    snapshot_date,
    ingested_at_timestamp,
    snapshot_try
FROM classified
WHERE rn = 1
