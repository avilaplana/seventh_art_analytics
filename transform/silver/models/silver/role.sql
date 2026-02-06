WITH roles_array AS (
  SELECT    
    SPLIT(TRIM(primaryProfession), ',') AS roles
  FROM {{ source('bronze', 'name_basics') }}
),

roles_distinct AS (
  SELECT
      DISTINCT(
              CASE
                  WHEN role = '\\N' OR role is NULL THEN 'UNKNOWN'
                  ELSE role
              END) AS role
  FROM roles_array
  LATERAL VIEW explode(roles) t AS role
  ORDER BY role ASC
), 

category_distinct AS (
  SELECT
    DISTINCT category AS role
  FROM {{ source('bronze', 'title_principals') }}
  ORDER BY role ASC
),

all_roles AS (
  SELECT role FROM roles_distinct 
  UNION 
  SELECT role FROM category_distinct
  ORDER BY role ASC
)

SELECT
UUID() AS role_id,
role AS role_name
FROM all_roles
