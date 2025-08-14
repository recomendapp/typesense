import os
import psycopg2
import typesense
from dotenv import load_dotenv

BATCH_SIZE = 100000

def sync_tv_series():
    print("--- Starting full sync for 'tv_series' collection ---")
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

        with db_conn.cursor(name='server_side_cursor_tv_series') as cursor:
            cursor.itersize = BATCH_SIZE

            sql_query = """
                SELECT
                    s.id,
                    s.original_name,
                    s.popularity::float AS popularity,
                    COALESCE(g.genre_ids, '{}') AS genre_ids,
                    s.number_of_episodes,
                    s.number_of_seasons,
                    s.vote_average::float AS vote_average,
                    s.vote_count::int AS vote_count,
                    s.status,
                    s.type,
                    first.first_air_ts,
                    last.last_air_ts,
                    COALESCE(names.names, '{}') AS names
                FROM public.tmdb_tv_series s
                LEFT JOIN LATERAL (
                    SELECT ARRAY_REMOVE(ARRAY_AGG(DISTINCT btrim(t.name)), NULL) AS names
                    FROM public.tmdb_tv_series_translations t
                    WHERE t.serie_id = s.id
                      AND t.name IS NOT NULL
                      AND btrim(t.name) <> ''
                ) AS names ON TRUE
                LEFT JOIN LATERAL (
                    SELECT ARRAY_AGG(DISTINCT sg.genre_id)::int[] AS genre_ids
                    FROM public.tmdb_tv_series_genres sg
                    WHERE sg.serie_id = s.id
                ) AS g ON TRUE
                LEFT JOIN LATERAL (
                    SELECT EXTRACT(EPOCH FROM s.first_air_date)::bigint AS first_air_ts
                ) AS first ON TRUE
                LEFT JOIN LATERAL (
                    SELECT EXTRACT(EPOCH FROM s.last_air_date)::bigint AS last_air_ts
                ) AS last ON TRUE
                ORDER BY s.id;
            """
            cursor.execute(sql_query)

            while True:
                records = cursor.fetchmany(BATCH_SIZE)
                if not records:
                    break

                documents = []
                for (serie_id, original_name, popularity, genre_ids,
                     num_episodes, num_seasons, vote_average, vote_count,
                     status, type_, first_air_ts, last_air_ts, names) in records:

                    nset = set()
                    if names:
                        for n in names:
                            if n and n.strip():
                                nset.add(n.strip())
                    if original_name and original_name.strip():
                        nset.add(original_name.strip())

                    doc = {
                        'id': str(serie_id),
                        'original_name': original_name or "",
                        'names': list(nset),
                        'popularity': float(popularity) if popularity is not None else 0.0,
                        'genre_ids': [int(g) for g in (genre_ids or [])],
                    }
                    if num_episodes is not None:
                        doc['number_of_episodes'] = int(num_episodes)
                    if num_seasons is not None:
                        doc['number_of_seasons'] = int(num_seasons)
                    if vote_average is not None:
                        doc['vote_average'] = float(vote_average)
                    if vote_count is not None:
                        doc['vote_count'] = int(vote_count)
                    if status:
                        doc['status'] = status
                    if type_:
                        doc['type'] = type_
                    if first_air_ts is not None:
                        doc['first_air_date'] = int(first_air_ts)
                    if last_air_ts is not None:
                        doc['last_air_date'] = int(last_air_ts)

                    documents.append(doc)

                print(f"Importing {len(documents)} documents...")
                results = typesense_client.collections['tv_series'].documents.import_(
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

    print(f"--- Sync complete. Total TV series synced: {total_synced} ---")

if __name__ == '__main__':
    sync_tv_series()
