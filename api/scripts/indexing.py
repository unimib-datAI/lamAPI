import os
import time
import json
import sys
import traceback
import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

from pymongo import MongoClient
from tqdm import tqdm

def index_documents(es, buffer, max_retries=5):
    for attempt in range(max_retries):
        try:
            bulk(es, buffer)
            break  # Exit the loop if the bulk operation was successful
        except BulkIndexError as e:
            # Log detailed information about the documents that failed to index
            print(f"Bulk indexing error on attempt {attempt + 1}: {e.errors}")
            # Extract and log more detailed info for each error
            for error_detail in e.errors:
                action, error_info = list(error_detail.items())[0]
                print(f"Failed action: {action}")
                print(f"Error details: {error_info}")
            time.sleep(5)
        except Exception as e:
            # Handle other exceptions
            print(f"An unexpected error occurred during indexing on attempt {attempt + 1}: {str(e)}")
            traceback.print_exc()  # Print the full traceback for unexpected errors
            time.sleep(5)
    else:
        print("Max retries exceeded. Failed to index some documents.")

def generate_dot_notation_options(name):
    words = name.split()
    num_words = len(words)
    options = []

    for i in range(num_words):
        abbreviated_parts = []
        for j in range(num_words - 1):
            if j < i:
                abbreviated_parts.append(words[j][0] + '.')
            else:
                abbreviated_parts.append(words[j])
        
        option = ' '.join(abbreviated_parts + [words[-1]])
        options.append(option)
    
    return options

def create_elasticsearch_client(endpoint, port):
    return Elasticsearch(
        hosts=f'http://{endpoint}:{port}',
        request_timeout=60,
        max_retries=10, 
        retry_on_timeout=True
    )

def create_mongo_client(endpoint, port, username, password):
    return MongoClient(endpoint, int(port), username=username, password=password)

def print_usage():
    print("Usage:")
    print("  python indexing_script.py index <DB_NAME> <COLLECTION_NAME> <MAPPING_FILE>")
    print("  python indexing_script.py status")
    print("  python indexing_script.py list_databases")
    print("  python indexing_script.py list_collections <DB_NAME>")
    print("Parameters:")
    print("  <DB_NAME>          : The name of the MongoDB database.")
    print("  <COLLECTION_NAME>  : The name of the MongoDB collection to index.")
    print("  <MAPPING_FILE>     : The path to the Elasticsearch mapping JSON file.")

def index_data(es, mongo_client, db_name, collection_name, mapping, batch_size=10000):
    documents_c = mongo_client[db_name][collection_name]

    # Find the document with the maximum popularity value
    max_popularity_doc = documents_c.find_one(sort=[("popularity", -1)])
    if max_popularity_doc:
        max_popularity = max_popularity_doc["popularity"]
        print(f"The maximum popularity is: {max_popularity}")
    else:
        raise Exception("No documents found in the collection or popularity field is missing.")

    # Create the index in Elasticsearch, using db_name without trailing numbers
    index_name = re.sub(r'\d+$', '', db_name)
    if es.indices.exists(index=index_name):
        print(f"Index {index_name} exists. Deleting it...")
        es.indices.delete(index=index_name)
    print(f"Creating index {index_name}...")
    es.indices.create(index=index_name, body={"settings": mapping["settings"], "mappings": mapping["mappings"]})

    total_docs = documents_c.estimated_document_count()
    results = documents_c.find({})
    
    buffer = []
    index = 0
    for item in tqdm(results, total=total_docs):
        doc = {
            "_op_type": "index",
            "_index": index_name,
            "_id": index,
        }
        # Add fields dynamically based on the mapping
        for field in mapping["mappings"]["properties"]:
            doc[field] = item.get(field, "")
            if field == "name" and mapping["mappings"]["properties"][field]["type"] == "text":
                if item.get("NERtype") == "PERS" and "labels" in item:
                    name = item["labels"].get("en")
                    if name is not None:
                        name_abbrevations = generate_dot_notation_options(name)
                        doc[field] = list(set(doc[field] + name_abbrevations))
        
        buffer.append(doc)
        index += 1

        if len(buffer) >= batch_size:
            index_documents(es, buffer)
            buffer = []
                
    if len(buffer) > 0:
        index_documents(es, buffer)

def show_status(mongo_client, es):
    print("MongoDB Status:")
    print(mongo_client.server_info())
    print("\nElasticsearch Status:")
    print(es.info())

def list_databases(mongo_client):
    print("Available Databases:")
    for db in mongo_client.list_database_names():
        print(f"  - {db}")

def list_collections(mongo_client, db_name):
    if db_name in mongo_client.list_database_names():
        print(f"Collections in database '{db_name}':")
        for coll in mongo_client[db_name].list_collection_names():
            print(f"  - {coll}")
    else:
        print(f"Database '{db_name}' not found.")

def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    action = sys.argv[1]

    ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")
    MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
    MONGO_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
    MONGO_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]

    es = create_elasticsearch_client(ELASTIC_ENDPOINT, ELASTIC_PORT)
    mongo_client = create_mongo_client(MONGO_ENDPOINT, MONGO_ENDPOINT_PORT, MONGO_USERNAME, MONGO_PASSWORD)

    try:
        if action == "index":
            if len(sys.argv) != 5:
                print_usage()
                sys.exit(1)
            
            db_name = sys.argv[2]
            collection_name = sys.argv[3]
            mapping_file = sys.argv[4]

            with open(mapping_file, 'r') as file:
                mapping = json.load(file)

            index_data(es, mongo_client, db_name, collection_name, mapping)
            print('All Finished')

        elif action == "status":
            show_status(mongo_client, es)
        
        elif action == "list_databases":
            list_databases(mongo_client)

        elif action == "list_collections":
            if len(sys.argv) != 3:
                print_usage()
                sys.exit(1)
            db_name = sys.argv[2]
            list_collections(mongo_client, db_name)

        else:
            print_usage()
            sys.exit(1)

    except Exception as e:
        print(e)
        traceback.print_exc()
        print("An error occurred. Exiting...")
        sys.exit(1)

if __name__ == "__main__":
    main()