SELECT
	t.title_id, 
	t.primary_title, 
	t.original_title, 
	tt.title_type_name, 
	t.is_adult, 
	t.release_year, 
	t.duration_minutes
FROM {{ref('title')}} t
JOIN {{ref('title_type')}} tt ON t.title_type_id = tt.title_type_id
