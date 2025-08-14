import os
import psycopg2
import typesense
from dotenv import load_dotenv

BATCH_SIZE = 100000

def sync_persons():
    print("--- Starting full sync for 'persons' collection ---")
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

        with db_conn.cursor(name='server_side_cursor_persons') as cursor:
            cursor.itersize = BATCH_SIZE

            sql_query = """
                SELECT
                    p.id,
                    p.name,
                    p.popularity::float AS popularity,
                    p.known_for_department,
                    COALESCE(aka.names, '{}') AS also_known_as
                FROM public.tmdb_person p
                LEFT JOIN LATERAL (
                    SELECT ARRAY_AGG(DISTINCT btrim(a.name)) AS names
                    FROM public.tmdb_person_also_known_as a
                    WHERE a.person = p.id
                ) AS aka ON TRUE
                ORDER BY p.id;
            """
            cursor.execute(sql_query)

            while True:
                records = cursor.fetchmany(BATCH_SIZE)
                if not records:
                    break

                documents = []
                for (person_id, name, popularity, known_for, also_known_as) in records:
                    tset = set()
                    if also_known_as:
                        for t in also_known_as:
                            if t and t.strip():
                                tset.add(t.strip())
                    if name and name.strip():
                        tset.add(name.strip())

                    doc = {
                        'id': str(person_id),
                        'name': name or "",
                        'also_known_as': list(tset),
                        'popularity': float(popularity) if popularity is not None else 0.0
                    }
                    if known_for:
                        doc['known_for_department'] = known_for

                    documents.append(doc)

                print(f"Importing {len(documents)} documents...")
                results = typesense_client.collections['persons'].documents.import_(
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

    print(f"--- Sync complete. Total persons synced: {total_synced} ---")

if __name__ == '__main__':
    sync_persons()
