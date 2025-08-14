# full-sync/users.py

import os
import psycopg2
import typesense
from dotenv import load_dotenv
from pathlib import Path

BATCH_SIZE = 5000

def sync_table():
    """
    Sync the 'users' table from PostgreSQL to Typesense.
    """
    print("--- Starting full sync for 'users' collection ---")
    
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


        with db_conn.cursor(name='server_side_cursor_users') as cursor:
            cursor.execute("SELECT id, username, full_name, followers_count FROM public.user ORDER BY created_at")
            
            while True:
                print(f"\nFetching a new batch of up to {BATCH_SIZE} users...")
                records = cursor.fetchmany(BATCH_SIZE)

                if not records:
                    print("No more users to fetch. Sync finished.")
                    break
                
                print(f"Fetched {len(records)} users. Transforming for Typesense...")

                documents = [
                    {
                        'id': str(record[0]),
                        'username': record[1],
                        'full_name': record[2],
                        'followers_count': record[3]
                    } for record in records
                ]
                
                print(f"Importing batch of {len(documents)} documents...")
                results = typesense_client.collections['users'].documents.import_(documents, {'action': 'upsert'})
                
                success_count = len([r for r in results if r['success']])
                total_synced += success_count
                print(f"✅ Successfully imported {success_count} documents in this batch.")

    except Exception as e:
        print(f"❌ An error occurred: {e}")
    finally:
        if db_conn:
            db_conn.close()
            print("\nDatabase connection closed.")
    
    print(f"\n--- Sync complete. Total documents synced: {total_synced} ---")

if __name__ == '__main__':
    sync_table()