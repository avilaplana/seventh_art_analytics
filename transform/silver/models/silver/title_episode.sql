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
    END AS episode_number
FROM {{ source ('bronze', 'title_episode') }}
