# parsing_cb.py

import os
import pandas as pd
from pymongo import MongoClient
from tqdm import tqdm
import dateutil.parser
import argparse

def create_indexes(db):
    index_specs = {
        'cache': ['name', 'limit', 'kg', 'fuzzy', 'types', 'kind', 'NERtype', 'language'],
        'items': ['id_entity', 'entity', 'kind', 'popularity'],
        'literals': ['id_entity', 'entity'],
        'types': ['id_entity', 'entity']
    }

    for collection, fields in index_specs.items():
        if collection == "cache":
            db[collection].create_index([(field, 1) for field in fields], unique=True, background=True)
        elif collection == "items":
            db[collection].create_index([('entity', 1), ('kind', 1)], unique=True)
        for field in fields:
            db[collection].create_index([(field, 1)])  # 1 for ascending order

def classify_value(value):
    try:
        dateutil.parser.isoparse(value)
        return 'DATETIME'
    except (ValueError, TypeError):
        pass
    try:
        float(value)
        return 'NUMBER'
    except (ValueError, TypeError):
        pass
    return 'STRING'

def parse_data(index, columns, data, additional_data):
    literals = {datatype: {} for datatype in ["STRING", "DATETIME", "NUMBER"]}
    types = {"P31": ["Organization"]}
    entity = data["uuid"]
    description = str(additional_data.get(entity, {}).get("description", ""))
    if description.lower() == "nan":
        description = ""
    popularity = additional_data.get(entity, {}).get("popularity", 0)
    all_labels = {"en": str(data["name"])}
    all_aliases = {"en": [str(data.get(f"alias{i}")) for i in range(1, 4) if data.get(f"alias{i}") and str(data.get(f"alias{i}")) != "nan"]}

    new_columns = set(columns) - {"uuid", "name", "alias1", "alias2", "alias3"}
    for column in new_columns:
        value = data[column]
        datatype = classify_value(value)
        if column not in literals[datatype]:
            literals[datatype][column] = []
        literals[datatype][column].append(value)

    join = {
        "items": {
            "id_entity": index,
            "entity": entity,
            "description": {"language": "en", "value": description},
            "labels": all_labels,
            "aliases": all_aliases,
            "types": types,
            "popularity": popularity,
            "kind": "entity",
            "NERtype": "ORG"
        },
        "literals": {
            "id_entity": index,
            "entity": entity,
            "literals": literals
        },
        "types": {
            "id_entity": index,
            "entity": entity,
            "types": types
        }
    }

    for key in buffer:
        buffer[key].append(join[key])

    if len(buffer["items"]) == BATCH_SIZE:
        flush_buffer(buffer)

def flush_buffer(buffer):
    for key in buffer:
        if buffer[key]:
            c_ref[key].insert_many(buffer[key])
            buffer[key] = []

def read_additional_data(file_path):
    additional_data = {}
    chunk_size = 1000

    total_lines = sum(1 for _ in open(file_path))
    total_chunks = total_lines // chunk_size + (1 if total_lines % chunk_size != 0 else 0)

    with tqdm(total=total_chunks, desc="Reading additional data") as pbar:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunk['rank'].fillna(0, inplace=True)  # Filling NaNs with 0
            for _, data in chunk.iterrows():
                entity_id = data["uuid"]
                additional_data[entity_id] = {
                    "url": data["cb_url"],
                    "description": data["description"],
                    "popularity": data["rank"]
                }
            pbar.update(1)
    return additional_data

def process_main_data(file_path, additional_data):
    chunk_size = 1000

    total_lines = sum(1 for _ in open(file_path))
    total_chunks = total_lines // chunk_size + (1 if total_lines % chunk_size != 0 else 0)

    with tqdm(total=total_chunks, desc="Processing main data") as pbar:
        index = 0
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            columns = chunk.columns
            for _, data in chunk.iterrows():
                parse_data(index, columns, data, additional_data)
                index += 1
            pbar.update(1)

    flush_buffer(buffer)

def main():
    parser = argparse.ArgumentParser(description="Process data and insert into MongoDB.")
    parser.add_argument("--db_name", type=str, required=True, help="Database name")
    parser.add_argument("--main_file", type=str, required=True, help="Main CSV file path")
    parser.add_argument("--additional_file", type=str, required=True, help="Additional CSV file path")
    parser.add_argument("--batch_size", type=int, default=1000, help="Batch size for insertion")

    args = parser.parse_args()

    global BATCH_SIZE, buffer, c_ref

    BATCH_SIZE = args.batch_size

    # Get MongoDB endpoint from environment variables
    MONGO_ENDPOINT = os.getenv("MONGO_ENDPOINT", "localhost")
    MONGO_ENDPOINT_PORT = int(os.getenv("MONGO_ENDPOINT_PORT", 27017))

    client = MongoClient(MONGO_ENDPOINT, MONGO_ENDPOINT_PORT)
    db = client[args.db_name]

    c_ref = {
        "items": db.items,
        "literals": db.literals,
        "types": db.types
    }

    create_indexes(db)

    buffer = {
        "items": [],
        "literals": [],
        "types": []
    }

    additional_data = read_additional_data(args.additional_file)
    process_main_data(args.main_file, additional_data)

    print("Finished processing and inserting documents.")

if __name__ == "__main__":
    main()