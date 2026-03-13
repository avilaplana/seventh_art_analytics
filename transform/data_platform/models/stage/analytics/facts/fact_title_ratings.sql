SELECT
	title_id, 
	average_rating,
	release_year
FROM {{ref('title')}}