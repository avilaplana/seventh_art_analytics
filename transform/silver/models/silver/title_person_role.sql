WITH title_principals_cleaned AS (
  SELECT
    tp.tconst AS title_id,
    tp.nconst AS person_id,
    tp.category,
    tp.ordering,
    CASE
      WHEN tp.job = '\\N' THEN NULL
      ELSE tp.job
    END AS job,
    CASE
      WHEN tp.characters = '\\N' THEN 'NOT DEFINED'
      ELSE TRIM(REPLACE(REPLACE(REPLACE(tp.characters, '[', ''), ']', ''), '\"',''))
    END AS characters,
    CAST(tp.snapshot_date AS DATE) AS snapshot_date,
    CAST(tp.ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    tp.snapshot_try   
FROM {{ source ('stage_raw', 'title_principals') }} tp
INNER JOIN {{ source ('stage_raw', 'name_basics') }} nb -- FILTER OUT persons that are not defined in name_basics
ON tp.nconst = nb.nconst
),

title_principals_with_map AS (
  SELECT
      title_id,
      person_id,
      category,
      job,
      COUNT(*) AS number_of_roles,
      map_from_entries(collect_set(struct(ordering, characters))) AS characters,
      snapshot_date,
      ingested_at_timestamp,
      snapshot_try
  FROM title_principals_cleaned
  GROUP BY
      title_id,
      person_id,
      category,
      job,
      snapshot_date,
      ingested_at_timestamp,
      snapshot_try
)

SELECT
    tpwm.title_id,
    tpwm.person_id,
    r.role_id,
    tpwm.job,
    tpwm.number_of_roles,
    tpwm.characters,
    tpwm.snapshot_date,
    tpwm.ingested_at_timestamp,
    tpwm.snapshot_try
FROM title_principals_with_map tpwm
LEFT JOIN {{ ref('role') }} r
ON tpwm.category = r.role_name
