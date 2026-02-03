WITH title_principals_cleaned AS (
  SELECT
    tconst AS title_id,
    nconst AS person_id,
    category,
    ordering,
    CASE
      WHEN job = '\\N' THEN NULL
      ELSE job
    END AS job,
    CASE
      WHEN characters = '\\N' THEN 'NOT DEFINED'
      ELSE TRIM(REPLACE(REPLACE(REPLACE(characters, '[', ''), ']', ''), '\"',''))
    END AS characters
FROM {{ source ('bronze', 'title_principals') }}
),
title_principals_with_map AS (
SELECT
    title_id,
    person_id,
    category,
    job,
    COUNT(*) AS number_of_roles,
    map_from_entries(collect_set(struct(ordering, characters))) AS characters
FROM title_principals_cleaned
GROUP BY
    title_id,
    person_id,
    category,
    job
)

SELECT
    tpwm.title_id,
    tpwm.person_id,
    r.role_id,
    tpwm.job,
    tpwm.number_of_roles,
    tpwm.characters
FROM title_principals_with_map tpwm
JOIN {{ ref('role') }} r
ON tpwm.category = r.role_name
