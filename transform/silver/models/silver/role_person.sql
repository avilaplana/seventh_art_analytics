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
                    WHEN role = '\\N' OR role is NULL THEN 'UNKNOWN'
                    ELSE role
            END AS role_name
    FROM roles_array
    LATERAL VIEW explode(roles) t AS role
)

SELECT 
    rp.person_id, 
    r.role_id 
FROM roles_person AS rp
INNER JOIN {{ ref('role') }} AS r  
ON rp.role_name = r.role_name