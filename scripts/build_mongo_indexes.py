#!/usr/bin/env python3

import os
import sys
import traceback
from pymongo import MongoClient

def create_mongo_client(endpoint, port):
    return MongoClient(endpoint, int(port))

def print_usage():
    print("Usage:")
    print("  python build_mongo_indexes.py create_indexes <DB_NAME>")
    print("  python build_mongo_indexes.py status")
    print("  python build_mongo_indexes.py list_databases")
    print("  python build_mongo_indexes.py list_collections <DB_NAME>")
    print("\nParameters:")
    print("  <DB_NAME>  : The name of the MongoDB database on which to create indexes.")

def show_status(mongo_client):
    """Print basic MongoDB server info."""
    print("MongoDB Status:")
    print(mongo_client.server_info())

def list_databases(mongo_client):
    """List all MongoDB databases."""
    print("Available Databases:")
    for db_name in mongo_client.list_database_names():
        print(f"  - {db_name}")

def list_collections(mongo_client, db_name):
    """List collections for a given database."""
    if db_name not in mongo_client.list_database_names():
        print(f"Database '{db_name}' not found.")
        return
    print(f"Collections in database '{db_name}':")
    for coll_name in mongo_client[db_name].list_collection_names():
        print(f"  - {coll_name}")

def create_indexes_in_mongo(mongo_client, db_name):
    """
    Hard-coded index specifications and creation logic, similar to your previous snippet.
    """
    # The fields to index for each collection
    index_specs = {
        "cache": ["name", "lastAccessed", "limit"],
        "items": ["id_entity", "entity", "category", "popularity"],
        "literals": ["id_entity", "entity"],
        "objects": ["id_entity", "entity"],
        "types": ["id_entity", "entity"],
        "bow": ["id"]
    }

    db = mongo_client[db_name]
    existing_collections = db.list_collection_names()

    for collection, fields in index_specs.items():
        if collection not in existing_collections:
            print(f"Collection '{collection}' not found in '{db_name}'. Skipping.")
            continue

        print(f"\nCreating indexes for collection '{collection}' in database '{db_name}'...")
        coll = db[collection]

        # Special indexes for certain collections
        if collection == "cache":
            # Unique compound index on these fields
            idx_name = coll.create_index(
                [
                    ("name", 1),
                    ("limit", 1),
                    ("kg", 1),
                    ("fuzzy", 1),
                    ("types", 1),
                    ("kind", 1),
                    ("NERtype", 1),
                    ("language", 1),
                ],
                unique=True,
                background=True
            )
            print(f"  - Created special unique index on 'cache': {idx_name}")

        elif collection == "items":
            idx_name = coll.create_index(
                [("entity", 1), ("kind", 1)],
                unique=True,
                background=True
            )
            print(f"  - Created special unique index on 'items': {idx_name}")

        elif collection == "bow":
            idx_name = coll.create_index(
                [("text", 1), ("id", 1)],
                unique=True,
                background=True
            )
            print(f"  - Created special unique index on 'bow': {idx_name}")

        # Generic indexes for the fields listed in index_specs
        for field in fields:
            idx_name = coll.create_index([(field, 1)], background=True)
            print(f"  - Created ascending index on '{field}': {idx_name}")

    print("\nAll specified indexes have been created.")

def main():
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    action = sys.argv[1]

    # Load environment variables for Mongo
    # Example: export MONGO_ENDPOINT="localhost:27017"
    MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
    mongo_client = create_mongo_client(MONGO_ENDPOINT, MONGO_ENDPOINT_PORT)

    try:
        if action == "create_indexes":
            if len(sys.argv) != 3:
                print_usage()
                sys.exit(1)
            
            db_name = sys.argv[2]
            create_indexes_in_mongo(mongo_client, db_name)
            print("\nIndex creation complete.")

        elif action == "status":
            show_status(mongo_client)

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
        print(f"\nError: {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()