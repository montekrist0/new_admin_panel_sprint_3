query_film_work = """
WITH modified_genres AS (
    SELECT
        gfw.film_work_id, g.modified
    FROM
        content.genre_film_work gfw
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
    WHERE g.modified > '{last_md_date}'
), modified_persons AS (
    SELECT
        pfw.film_work_id, p.modified
    FROM
        content.person_film_work pfw
        LEFT JOIN content.person p ON p.id = pfw.person_id
    WHERE p.modified > '{last_md_date}'
), modified_filmworks AS (
    SELECT
        id as film_work_id, modified
    FROM
        content.film_work
    WHERE modified > '{last_md_date}'
), ordered_records AS (
    SELECT film_work_id, modified FROM modified_genres
    UNION
    SELECT * FROM modified_persons
    UNION
    SELECT * FROM modified_filmworks
)
SELECT
   fw.id,
   fw.rating AS imdb_rating,
   array_agg(DISTINCT g.name) as genre,
   fw.title,
   fw.description,
   ordered_records.modified,
   COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'director' AND p.full_name IS NOT NULL), '{{}}') AS director,
   COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'actor' AND p.full_name IS NOT NULL), '{{}}') AS actors_names,
   COALESCE(array_agg(DISTINCT p.full_name) FILTER (WHERE pfw.role = 'writer' AND p.full_name IS NOT NULL), '{{}}') AS writers_names,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE pfw.role = 'actor'),
       '[]'
   ) as actors,
   COALESCE (
       json_agg(
           DISTINCT jsonb_build_object(
               'id', p.id,
               'name', p.full_name
           )
       ) FILTER (WHERE pfw.role = 'writer'),
       '[]'
   ) as writers
FROM ordered_records
LEFT JOIN content.film_work fw ON ordered_records.film_work_id = fw.id
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
GROUP BY fw.id, ordered_records.modified
ORDER BY ordered_records.modified
LIMIT {batch_size};
"""
