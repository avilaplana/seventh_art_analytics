SELECT
	title_id,
	person_id,
	role_id
FROM {{ref('title_person_role')}}