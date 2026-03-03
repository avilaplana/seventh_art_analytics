WITH roles_array AS (
  SELECT    
    SPLIT(TRIM(primaryProfession), ',') AS roles,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    snapshot_try
  FROM {{ source('stage_bronze', 'name_basics') }}
),

roles_distinct AS (

  SELECT
      DISTINCT
              snapshot_date,
              ingested_at_timestamp,
              snapshot_try,
              CASE
                  WHEN role = '\\N' THEN NULL
                  ELSE role
              END AS role
  FROM roles_array
  LATERAL VIEW explode(roles) t AS role
  ORDER BY role ASC
), 

category_distinct AS (
  SELECT
    DISTINCT 
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    snapshot_try,
    category AS role
  FROM {{ source('stage_bronze', 'title_principals') }}
  ORDER BY role ASC
),

all_roles AS (
  SELECT role, snapshot_date, snapshot_try, ingested_at_timestamp  FROM roles_distinct 
  WHERE role IS NOT NULL
  UNION 
  SELECT role, snapshot_date, snapshot_try, ingested_at_timestamp FROM category_distinct
  ORDER BY role ASC
)

SELECT
  UUID() AS role_id,
  role AS role_name,
  snapshot_date,
  ingested_at_timestamp,
  snapshot_try
FROM all_roles
