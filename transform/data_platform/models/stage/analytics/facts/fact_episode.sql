SELECT
	episode_title_id,
	series_title_id,
	season_number,
	episode_number
FROM {{ref('title_episode')}}