SELECT
    tconst as episode_title_id,
    parentTconst as series_title_id,
    CASE
        WHEN seasonNumber = '\\N' THEN NULL
        ELSE CAST(seasonNumber AS INT)
    END AS season_number,
    CASE
        WHEN episodeNumber = '\\N' THEN NULL
        ELSE CAST(episodeNumber AS INT)
    END AS episode_number,
    CAST(snapshot_date AS DATE) AS snapshot_date,
    CAST(ingested_at_timestamp AS TIMESTAMP) AS ingested_at_timestamp,
    snapshot_try
FROM {{ source ('stage_bronze', 'title_episode') }}
