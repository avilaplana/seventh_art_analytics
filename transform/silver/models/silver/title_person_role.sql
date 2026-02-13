{{ config(
    materialized='incremental',
    unique_key=['title_id', 'person_id', 'role_id']
) }}

WITH latest_snapshot_name_basics AS (
    SELECT *
    FROM {{ source('bronze', 'name_basics') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'name_basics') }}
    )
),
latest_snapshot_title_principals AS (
    SELECT *
    FROM {{ source('bronze', 'title_principals') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_principals') }}
    )
),
title_principals_cleaned AS (
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
    END AS characters
FROM  latest_snapshot_title_principals tp
INNER JOIN latest_snapshot_name_basics nb -- FILTER OUT persons that are not defined in name_basics
ON tp.nconst = nb.nconst
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
LEFT JOIN {{ ref('role') }} r
ON tpwm.category = r.role_name
