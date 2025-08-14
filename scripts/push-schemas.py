import os
import json
import typesense
from dotenv import load_dotenv

def push_schemas():
    """
    Push schemas to Typesense.
    """
    load_dotenv()

    # Init client
    typesense_client = typesense.Client({
        'nodes': [{
            'host': os.getenv('TYPESENSE_HOST'),
            'port': os.getenv('TYPESENSE_PORT', 8108),
            'protocol': os.getenv('TYPESENSE_PROTOCOL', 'http')
        }],
        'api_key': os.getenv('TYPESENSE_API_KEY'),
        'connection_timeout_seconds': 5
    })

    try:
        print("Fetching existing collections from Typesense server...")
        existing_collections_data = typesense_client.collections.retrieve()
        existing_collection_names = {collection['name'] for collection in existing_collections_data}
        print(f"Found existing collections: {existing_collection_names or 'None'}")
    except Exception as e:
        print(f"Could not fetch existing collections: {e}")
        return

    schemas_dir = os.path.join(os.path.dirname(__file__), '..', 'schemas')
    print(f"\nReading local schemas from: {schemas_dir}")
    
    local_schemas = []
    for filename in os.listdir(schemas_dir):
        if filename.endswith('.json'):
            filepath = os.path.join(schemas_dir, filename)
            with open(filepath, 'r') as f:
                local_schemas.append(json.load(f))

    for schema in local_schemas:
        collection_name = schema['name']
        print(f"\nProcessing local schema: '{collection_name}'")
        
        if collection_name in existing_collection_names:
            print(f"  - ✅ SKIPPING: Collection '{collection_name}' already exists.")
        else:
            print(f"  - 🆕 CREATING: Collection '{collection_name}'...")
            try:
                typesense_client.collections.create(schema)
                print(f"  - ✅ SUCCESS: Collection '{collection_name}' created.")
            except Exception as e:
                print(f"  - ❌ FAILED to create collection '{collection_name}': {e}")

    print("\nSchema check complete.")


if __name__ == '__main__':
    push_schemas()