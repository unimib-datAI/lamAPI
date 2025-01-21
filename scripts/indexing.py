import json
import os
import re
import sys
import time
import traceback
from multiprocessing import Pool, cpu_count

from elasticsearch import Elasticsearch
from elasticsearch.helpers import BulkIndexError, bulk
from pymongo import MongoClient
from tqdm import tqdm


def index_documents(es_host, es_port, buffer, max_retries=5):
    es = Elasticsearch(
        hosts=f"http://{es_host}:{es_port}",
        request_timeout=60,
        max_retries=10,
        retry_on_timeout=True,
    )
    for attempt in range(max_retries):
        try:
            bulk(es, buffer)
            break  # Exit the loop if the bulk operation was successful
        except BulkIndexError as e:
            print(f"Bulk indexing error on attempt {attempt + 1}: {e.errors}")
            for error_detail in e.errors:
                action, error_info = list(error_detail.items())[0]
                print(f"Failed action: {action}")
                print(f"Error details: {error_info}")
            time.sleep(5)
        except Exception as e:
            print(
                f"An unexpected error occurred during indexing on attempt {attempt + 1}: {str(e)}"
            )
            traceback.print_exc()
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
                abbreviated_parts.append(words[j][0] + ".")
            else:
                abbreviated_parts.append(words[j])

        option = " ".join(abbreviated_parts + [words[-1]])
        options.append(option)

    return options


def create_elasticsearch_client(endpoint, port):
    return Elasticsearch(
        hosts=f"http://{endpoint}:{port}",
        request_timeout=60,
        max_retries=10,
        retry_on_timeout=True,
    )


def create_mongo_client(endpoint, port):
    MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
    MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
    
    return MongoClient(endpoint, int(port),
    username=MONGO_ENDPOINT_USERNAME,
    password=MONGO_ENDPOINT_PASSWORD)


def print_usage():
    print("Usage:")
    print(
        "  python indexing_script.py index <DB_NAME> <COLLECTION_NAME> <MAPPING_FILE>"
    )
    print("  python indexing_script.py status")
    print("  python indexing_script.py list_databases")
    print("  python indexing_script.py list_collections <DB_NAME>")
    print("Parameters:")
    print("  <DB_NAME>          : The name of the MongoDB database.")
    print("  <COLLECTION_NAME>  : The name of the MongoDB collection to index.")
    print("  <MAPPING_FILE>     : The path to the Elasticsearch mapping JSON file.")


def process_batch(args):
    es_host, es_port, batch = args
    index_documents(es_host, es_port, batch)


def index_data(
    es_host,
    es_port,
    mongo_client,
    db_name,
    collection_name,
    mapping,
    batch_size=100000,
    max_threads=None,
):
    if max_threads is None:
        max_threads = cpu_count() - 1

    documents_c = mongo_client[db_name][collection_name]

    max_popularity_doc = documents_c.find_one(sort=[("popularity", -1)])
    if max_popularity_doc:
        max_popularity = max_popularity_doc["popularity"]
        print(f"The maximum popularity is: {max_popularity}")
    else:
        raise Exception(
            "No documents found in the collection or popularity field is missing."
        )

    index_name = re.sub(r"\d+$", "", db_name)
    es_client = create_elasticsearch_client(es_host, es_port)
    if es_client.indices.exists(index=index_name):
        print(f"Index {index_name} exists. Deleting it...")
        es_client.indices.delete(index=index_name)

    print(f"Creating index {index_name}...")
    es_client.indices.create(
        index=index_name, settings=mapping["settings"], mappings=mapping["mappings"]
    )

    # Disable refresh interval and replicas temporarily
    es_client.indices.put_settings(
        index=index_name,
        settings={"index": {"refresh_interval": "-1", "number_of_replicas": 0}},
    )

    total_docs = documents_c.estimated_document_count()
    results = documents_c.find({})

    buffer = []
    batches = []
    pbar = tqdm(total=total_docs, desc="Indexing documents")
    _id = 0
    for item in results:
        id_entity = item.get("entity")
        labels = item.get("labels", {})
        aliases = item.get("aliases", {})
        description = item.get("description", {}).get("value", None)
        NERtype = item.get("NERtype", None)
        WD_type = item.get("WD_type", None)
        ext_WD_type = item.get("ext_WD_type", None)
        types = item.get("types", {}).get("P31", [])
        kind = item.get("kind", None)
        popularity = int(item.get("popularity", 0))
        unique_labels = {}

        for lang, name in labels.items():
            key = name.lower()
            if key not in unique_labels:
                unique_labels[key] = {"name": name, "languages": [], "is_alias": False}
            unique_labels[key]["languages"].append(lang)

        for lang, alias_list in aliases.items():
            for alias in alias_list:
                key = alias.lower()
                if (
                    key in unique_labels and not unique_labels[key]["is_alias"]
                ):  # Skip if the alias is already a label
                    continue
                if key not in unique_labels:
                    unique_labels[key] = {
                        "name": alias,
                        "languages": [],
                        "is_alias": True,
                    }
                unique_labels[key]["languages"].append(lang)

        all_names = []
        for _, value in unique_labels.items():
            name = value["name"]
            languages = value["languages"]
            is_alias = value["is_alias"]
            all_names.append(
                {"name": name, "language": languages, "is_alias": is_alias}
            )

        if NERtype == "PERS":
            name = labels.get("en")
            if name is not None:
                name_abbreviations = generate_dot_notation_options(name)
                for abbrev in name_abbreviations:
                    all_names.append(
                        {"name": abbrev, "language": ["en"], "is_alias": True}
                    )

        for name_entry in all_names:
            name = name_entry["name"]
            language = name_entry["language"]
            is_alias = name_entry["is_alias"]
            doc = {
                "_op_type": "index",
                "_index": index_name,
                "_id": _id,
                "id": id_entity,
                "name": name,
                "language": language,
                "is_alias": is_alias,
                "description": description,
                "kind": kind,
                "NERtype": NERtype,
                "WD_type": WD_type,
                "ext_WD_type": ext_WD_type,
                "types": " ".join(types),
                "length": len(name),
                "ntoken": len(name.split(" ")),
                "popularity": round(popularity / max_popularity, 2),
            }
            _id += 1
            buffer.append(doc)

            if len(buffer) >= batch_size:
                batches.append(buffer)
                buffer = []

                if len(batches) >= max_threads:
                    with Pool(max_threads) as pool:
                        pool.map(
                            process_batch,
                            [(es_host, es_port, batch) for batch in batches],
                        )
                    batches = []

        pbar.update(1)

    if len(buffer) > 0:
        batches.append(buffer)

    if len(batches) > 0:
        with Pool(max_threads) as pool:
            pool.map(process_batch, [(es_host, es_port, batch) for batch in batches])

    pbar.close()
    # Enable refresh interval
    es_client.indices.put_settings(
        index=index_name, settings={"index": {"refresh_interval": "1s"}}
    )


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
    print(sys.argv)
    if len(sys.argv) < 2:
        print_usage()
        sys.exit(1)

    action = sys.argv[1]

    ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")
    #ELASTIC_ENDPOINT="lamapi_elastic"
    MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
    es = create_elasticsearch_client(ELASTIC_ENDPOINT, ELASTIC_PORT)
    mongo_client = create_mongo_client(MONGO_ENDPOINT, MONGO_ENDPOINT_PORT)

    try:
        if action == "index":
            if len(sys.argv) != 5:
                print_usage()
                sys.exit(1)

            db_name = sys.argv[2]
            collection_name = sys.argv[3]
            mapping_file = sys.argv[4]

            with open(mapping_file, "r") as file:
                mapping = json.load(file)

            # Perform indexing
            index_data(
                ELASTIC_ENDPOINT,
                ELASTIC_PORT,
                mongo_client,
                db_name,
                collection_name,
                mapping,
            )

            print("All Finished")

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
