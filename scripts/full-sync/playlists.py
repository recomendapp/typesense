# full-sync/playlists.py

import os
import psycopg2
import typesense
from dotenv import load_dotenv
from pathlib import Path

BATCH_SIZE = 5000

def sync_playlists():
	"""
    Sync the 'playlists' table from PostgreSQL to Typesense.
    """
	print("--- Starting full sync for 'playlists' collection ---")

	load_dotenv()

	typesense_client = typesense.Client({
		'nodes': [{'host': os.getenv('TYPESENSE_HOST'), 'port': os.getenv('TYPESENSE_PORT', '443'), 'protocol': os.getenv('TYPESENSE_PROTOCOL', 'https') }],
		'api_key': os.getenv('TYPESENSE_API_KEY'),
		'connection_timeout_seconds': 10
	})
	
	db_conn = None
	total_synced = 0

	try:
		print("Connecting to PostgreSQL database...")
		db_conn = psycopg2.connect(os.getenv('POSTGRES_CONNECTION_STRING'))
		print("✅ Connection successful.")

		with db_conn.cursor(name='server_side_cursor_playlists') as cursor:
			sql_query = """
				SELECT
					p.id,
					p.title,
					p.description,
					p.likes_count,
					p.items_count,
					EXTRACT(EPOCH FROM p.created_at)::bigint, -- Convertir la date en timestamp Unix
					EXTRACT(EPOCH FROM p.updated_at)::bigint, -- Convertir la date en timestamp Unix
					p.private,
					p.user_id,
					ARRAY_REMOVE(ARRAY_AGG(pg.user_id::text), NULL) AS guest_ids
				FROM
					public.playlists p
				LEFT JOIN
					public.playlist_guests pg ON p.id = pg.playlist_id
				GROUP BY
					p.id
				ORDER BY
					p.created_at;
			"""
			cursor.execute(sql_query)
			
			while True:
				print(f"\nFetching a new batch of up to {BATCH_SIZE} playlists...")
				records = cursor.fetchmany(BATCH_SIZE)

				if not records:
					print("No more playlists to fetch.")
					break

				print(f"Fetched {len(records)} playlists. Transforming for Typesense...")

				documents = [
					{
						'id': str(record[0]),
						'title': record[1],
						'description': record[2],
						'likes_count': record[3],
						'items_count': record[4],
						'created_at': record[5],
						'updated_at': record[6],
						'is_private': record[7],
						'owner_id': str(record[8]),
						'guest_ids': [str(guest_id) for guest_id in record[9]]
					} for record in records
				]
				
				print(f"Records is {documents}")
				
				print(f"Importing batch of {len(documents)} documents...")
				results = typesense_client.collections['playlists'].documents.import_(documents, {'action': 'upsert'})
				
				success_count = len([r for r in results if r['success']])
				total_synced += success_count
				print(f"✅ Successfully imported {success_count} documents in this batch.")

	except Exception as e:
		print(f"❌ An error occurred: {e}")
	finally:
		if db_conn:
			db_conn.close()
			print("\nDatabase connection closed.")
	
	print(f"\n--- Sync complete. Total playlists synced: {total_synced} ---")

if __name__ == '__main__':
	sync_playlists()