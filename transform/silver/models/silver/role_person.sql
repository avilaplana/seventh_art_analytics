WITH roles_array AS (
  SELECT
    nconst AS person_id,    
    SPLIT(TRIM(primaryProfession), ',') AS roles
  FROM {{ source('bronze', 'name_basics') }}
),

roles_person AS (
    SELECT
        DISTINCT 
            person_id, 
            CASE
                    WHEN role = '\\N' THEN NULL
                    ELSE role
            END AS role_name
    FROM roles_array
    LATERAL VIEW explode(roles) t AS role
)

SELECT 
    rp.person_id, 
    r.role_id 
FROM roles_person AS rp
LEFT JOIN {{ ref('role') }} AS r
ON rp.role_name = r.role_name
WHERE rp.role_name IS NOT NULL