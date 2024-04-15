import bz2
import json
import os
import sys
import traceback
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime


def create_indexes(db):
    # Specify the collections and their respective fields to be indexed
    index_specs = {
        'cache': ['cell', 'lastAccessed'],  # Example: Indexing 'cell' and 'type' fields in 'cache' collection
        'items': ['id_entity', 'entity', 'category', 'popularity'],
        'literals': ['id_entity', 'entity'],
        'mappings': ['curid', 'wikipedia_id', 'wikidata_id', 'dbpedia_id'],
        'objects': ['id_entity', 'entity'],
        'types': ['id_entity', 'entity']
    }

    for collection, fields in index_specs.items():
        if collection == "cache":
            db[collection].create_index([('cell', 1), ('fuzzy', 1), ('type', 1), ('kg', 1), ('limit', 1)], unique=True)
        elif collection == "items":
            db[collection].create_index([('entity', 1), ('category', 1)], unique=True)    
        for field in fields:
            db[collection].create_index([(field, 1)])  # 1 for ascending order


# Initial Estimation
initial_estimated_average_size = 800  # Initial average size in bytes, can be adjusted
BATCH_SIZE = 100 # Number of entities to insert in a single batch

if len(sys.argv) < 2:
    print("Usage: python script_name.py <path_to_wikidata_dump>")
    sys.exit(1)

file_path = sys.argv[1]  # Get the file path from command line argument
compressed_file_size = os.path.getsize(file_path)
initial_total_lines_estimate = compressed_file_size / initial_estimated_average_size

file = bz2.BZ2File(file_path, "r")

# MongoDB connection setup
MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_ENDPOINT_PORT = int(MONGO_ENDPOINT_PORT)
MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
current_date = datetime.now()
formatted_date = current_date.strftime("%d%m%Y")
DB_NAME = f"wikidata{formatted_date}"

client = MongoClient(MONGO_ENDPOINT, MONGO_ENDPOINT_PORT, username=MONGO_ENDPOINT_USERNAME, password=MONGO_ENDPOINT_PASSWORD)
log_c = client.wikidata.log
items_c = client[DB_NAME].items
objects_c = client[DB_NAME].objects
literals_c = client[DB_NAME].literals
types_c = client[DB_NAME].types

c_ref = {
    "items": items_c,
    "objects":objects_c, 
    "literals":literals_c, 
    "types":types_c
}

create_indexes(client[DB_NAME])

buffer = {
    "items": [],
    "objects": [], 
    "literals": [], 
    "types": []
}

DATATYPES_MAPPINGS = {
    'external-id':'STRING',
    'quantity': 'NUMBER',
    'globe-coordinate': 'STRING',
    'string': 'STRING',
    'monolingualtext': 'STRING',
    'commonsMedia': 'STRING',
    'time': 'DATETIME',
    'url': 'STRING',
    'geo-shape': 'GEOSHAPE',
    'math': 'MATH',
    'musical-notation': 'MUSICAL_NOTATION',
    'tabular-data': 'TABULAR_DATA'
}
DATATYPES = list(set(DATATYPES_MAPPINGS.values()))
total_size_processed = 0
num_entities_processed = 0



def update_average_size(new_size):
    global total_size_processed, num_entities_processed
    total_size_processed += new_size
    num_entities_processed += 1
    return total_size_processed / num_entities_processed


def check_skip(obj, datatype):
    temp = obj.get("mainsnak", obj)
    if "datavalue" not in temp:
        return True

    skip = {
        "wikibase-lexeme",
        "wikibase-form",
        "wikibase-sense"
    }
    
    return datatype in skip


def get_value(obj, datatype):
    temp = obj.get("mainsnak", obj)
    if datatype == "globe-coordinate":
        latitude = temp["datavalue"]["value"]["latitude"]
        longitude = temp["datavalue"]["value"]["longitude"]
        value = f"{latitude},{longitude}"
    else:
        keys = {
            "quantity": "amount",
            "monolingualtext": "text",
            "time": "time",
        }
        if datatype in keys:
            key = keys[datatype]
            value = temp["datavalue"]["value"][key]
        else:
            value = temp["datavalue"]["value"]
    return value


def flush_buffer(buffer):
    for key in buffer:
        if len(buffer[key]) > 0:
            c_ref[key].insert_many(buffer[key])
            buffer[key] = []
            
            
def parse_data(item, i):
    entity = item["id"]
    labels = item.get("labels", {})
    aliases = item.get("aliases", {})
    description = item.get('descriptions', {}).get('en', {})
    category = "entity"
    sitelinks = item.get("sitelinks", {})
    popularity = len(sitelinks) if len(sitelinks) > 0 else 1
    
    all_labels = {}
    for lang in labels:
        all_labels[lang] = labels[lang]["value"]

    all_aliases = {}
    for lang in aliases:
        all_aliases[lang] = []
        for alias in aliases[lang]:
            all_aliases[lang].append(alias["value"])
        all_aliases[lang] = list(set(all_aliases[lang]))

    found = False
    for predicate in item["claims"]:
        if predicate == "P279":
            found = True

    if found:
        category = "type"
    if entity[0] == "P":
        category = "predicate"

    objects = {}
    literals = {datatype: {} for datatype in DATATYPES}
    types = {"P31": []}
    join = {
        "items": {
            "id_entity": i,
            "entity": entity,
            "description": description,
            "labels": all_labels,
            "aliases": all_aliases,
            "types": types,
            "popularity": popularity,
            "category": category,   # kind (entity, type or predicate)
            "NERtype": None # (ORG, LOC, PER or OTHERS)
        },
        "objects": { 
            "id_entity": i,
            "entity": entity,
            "objects":objects
        },
        "literals": { 
            "id_entity": i,
            "entity": entity,
            "literals": literals
        },
        "types": { 
            "id_entity": i,
            "entity": entity,
            "types": types
        },
    }

    predicates = item["claims"]
    for predicate in predicates:
        for obj in predicates[predicate]:
            datatype = obj["mainsnak"]["datatype"]

            if check_skip(obj, datatype):
                continue

            if datatype == "wikibase-item" or datatype == "wikibase-property":
                value = obj["mainsnak"]["datavalue"]["value"]["id"]

                if predicate == "P31" or predicate == "P106":
                    types["P31"].append(value)

                if value not in objects:
                    objects[value] = []
                objects[value].append(predicate)    
            else:
                value = get_value(obj, datatype)                
                lit = literals[DATATYPES_MAPPINGS[datatype]]

                if predicate not in lit:
                    lit[predicate] = []
                lit[predicate].append(value)   

    for key in buffer:
        buffer[key].append(join[key])            

    if len(buffer["items"]) == BATCH_SIZE:
        flush_buffer(buffer)


def parse_wikidata_dump():            
    global initial_total_lines_estimate
    pbar = tqdm(total=initial_total_lines_estimate)
    for i, line in enumerate(file):
        try:
            item = json.loads(line[:-2])  # Remove the trailing characters
            line_size = len(line)
            current_average_size = update_average_size(line_size)

            # Dynamically update the total based on the current average size
            pbar.total = round(compressed_file_size / current_average_size)
            pbar.update(1)

            parse_data(item, i)
        except json.decoder.JSONDecodeError:
            continue
        except Exception as e:
            traceback_str = traceback.format_exc()
            log_c.insert_one({"entity": item["id"], "error": str(e), "traceback_str": traceback_str})

    if len(buffer["items"]) > 0:
        flush_buffer(buffer)

    pbar.close()


def main():
    parse_wikidata_dump()
    final_average_size = total_size_processed / num_entities_processed
    print(f"Final average size of an entity: {final_average_size} bytes")
    # Optionally store this value for future use


if __name__ == "__main__":
    main()
