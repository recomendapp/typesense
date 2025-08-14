import os
import psycopg2
import typesense
from dotenv import load_dotenv

BATCH_SIZE = 100000

def sync_movies():
	print("--- Starting full sync for 'movies' collection ---")
	load_dotenv()

	typesense_client = typesense.Client({
		'nodes': [{
			'host': os.getenv('TYPESENSE_HOST'),
			'port': os.getenv('TYPESENSE_PORT', '443'),
			'protocol': os.getenv('TYPESENSE_PROTOCOL', 'https')
		}],
		'api_key': os.getenv('TYPESENSE_API_KEY'),
		'connection_timeout_seconds': 10
	})

	db_conn = None
	total_synced = 0

	try:
		print("Connecting to PostgreSQL...")
		db_conn = psycopg2.connect(os.getenv('POSTGRES_CONNECTION_STRING'))
		print("✅ Connected.")

		with db_conn.cursor(name='server_side_cursor_movies') as cursor:
			cursor.itersize = BATCH_SIZE

			sql_query = """
				SELECT
					m.id,
					m.original_title,
					m.popularity::float AS popularity,
					COALESCE(g.genre_ids, '{}') AS genre_ids,
					rt.runtime,                      
					rel.release_ts,                  -- timestamp Unix
					COALESCE(titles.titles, '{}') AS titles
				FROM public.tmdb_movie m
				LEFT JOIN LATERAL (
					SELECT ARRAY_REMOVE(ARRAY_AGG(DISTINCT btrim(t.title)), NULL) AS titles
					FROM public.tmdb_movie_translations t
					WHERE t.movie_id = m.id
					  AND t.title IS NOT NULL
					  AND btrim(t.title) <> ''
				) AS titles ON TRUE
				LEFT JOIN LATERAL (
					SELECT t.runtime
					FROM public.tmdb_movie_translations t
					WHERE t.movie_id = m.id
					  AND t.runtime IS NOT NULL AND t.runtime > 0
					ORDER BY (t.iso_639_1 = m.original_language) DESC, t.id
					LIMIT 1
				) AS rt ON TRUE
				LEFT JOIN LATERAL (
					SELECT EXTRACT(EPOCH FROM r.release_date)::bigint AS release_ts
					FROM public.tmdb_movie_release_dates r
					WHERE r.movie_id = m.id AND r.release_type IN (2,3)
					ORDER BY r.release_date ASC
					LIMIT 1
				) AS rel ON TRUE
				LEFT JOIN LATERAL (
					SELECT ARRAY_AGG(DISTINCT mg.genre_id)::int[] AS genre_ids
					FROM public.tmdb_movie_genres mg
					WHERE mg.movie_id = m.id
				) AS g ON TRUE
				ORDER BY m.id;
			"""
			cursor.execute(sql_query)

			while True:
				records = cursor.fetchmany(BATCH_SIZE)
				if not records:
					break

				documents = []
				for (movie_id, original_title, popularity, genre_ids, runtime, release_ts, titles) in records:
					tset = set()
					if titles:
						for t in titles:
							if t and t.strip():
								tset.add(t.strip())
					if original_title and original_title.strip():
						tset.add(original_title.strip())

					doc = {
						'id': str(movie_id),
						'original_title': original_title or "",
						'titles': list(tset),
						'popularity': float(popularity) if popularity is not None else 0.0,
						'genre_ids': [int(g) for g in (genre_ids or [])],
					}
					if runtime is not None:
						doc['runtime'] = int(runtime)
					if release_ts is not None:
						doc['release_date'] = int(release_ts)  # Unix timestamp

					documents.append(doc)

				print(f"Importing {len(documents)} documents...")
				results = typesense_client.collections['movies'].documents.import_(
					documents, {'action': 'upsert'}
				)
				success_count = sum(1 for r in results if r.get('success'))
				total_synced += success_count
				print(f"✅ Imported {success_count}/{len(documents)}")

	except Exception as e:
		print(f"❌ Error: {e}")
	finally:
		if db_conn:
			db_conn.close()
			print("DB connection closed.")

	print(f"--- Sync complete. Total movies synced: {total_synced} ---")

if __name__ == '__main__':
	sync_movies()
