SELECT
	title_id,
	region_id,
	language_id,
	title,
	ordering,
	is_original_title	
FROM  {{ref('title_localized')}}