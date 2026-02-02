SELECT 
  r.role_id, 
  tconst AS title_id, 
  nconst AS person_id
FROM {{ source('bronze', 'title_principals') }} tp
JOIN {{ ref('role') }} r
ON tp.category = r.role_name