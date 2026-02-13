{{ config(
    materialized='incremental',
    unique_key=['title_id', 'parent_title_id', 'seasonNumber', 'episodeNumber']
) }}

WITH latest_snapshot_title_episode AS (
    SELECT *
    FROM {{ source('bronze', 'title_episode') }}
    WHERE ingestion_date = (
        SELECT MAX(ingestion_date) 
        FROM {{ source('bronze', 'title_episode') }}
    )
)
SELECT
    tconst as title_id,
    parentTconst as parent_title_id,
    CASE
        WHEN seasonNumber = '\\N' THEN NULL
        ELSE seasonNumber
    END AS seasonNumber,
    CASE
        WHEN episodeNumber = '\\N' THEN NULL
        ELSE episodeNumber
    END AS episodeNumber
FROM latest_snapshot_title_episode
