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
FROM {{ source ('bronze', 'title_episode') }}
