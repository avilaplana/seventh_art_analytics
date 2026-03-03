WITH roles_array AS (
  SELECT
    nconst AS person_id,    
    SPLIT(TRIM(primaryProfession), ',') AS roles,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp
  FROM {{ source('stage_bronze', 'name_basics') }}
),

roles_person AS (
    SELECT
        DISTINCT 
            person_id, 
            CASE
                    WHEN role = '\\N' THEN NULL
                    ELSE role
            END AS role_name,
            snapshot_date,
            ingested_at_timestamp
    FROM roles_array
    LATERAL VIEW explode(roles) t AS role
)

SELECT 
    rp.person_id, 
    r.role_id,
    rp.snapshot_date,
    rp.ingested_at_timestamp
FROM roles_person AS rp
LEFT JOIN {{ ref('role') }} AS r
ON rp.role_name = r.role_name
WHERE rp.role_name IS NOT NULL