import os
import time
import subprocess
import json
import sys
import traceback
import re

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

from pymongo import MongoClient
from tqdm import tqdm

from conf import MAPPING



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


ELASTIC_USER = os.environ["ELASTICSEARCH_USERNAME"]
ELASTIC_PW = os.environ["ELASTIC_PASSWORD"]
ELASTIC_LIMITS = 100
ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")

try:
    db_name = sys.argv[1:][0]
    kg_name = match = re.match(r"([a-zA-Z]+)(\d+)", db_name).group(1)
except:
    sys.exit("Please provide a DB name as argument")

bashCommand = """
                openssl s_client -connect es01:9200 -servername es01 -showcerts </dev/null 2>/dev/null | 
                openssl x509 -fingerprint -sha256 -noout -in /dev/stdin
            """
p = subprocess.Popen(
    bashCommand, 
    stdout=subprocess.PIPE, shell=True)
output = p.communicate()
fingerprint = output[0].decode("UTF-8")
CERT_FINGERPRINT = fingerprint.split("=")[1][0:-1]


try:
    MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
    MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
    MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
    client = MongoClient(MONGO_ENDPOINT, int(MONGO_ENDPOINT_PORT), username=MONGO_ENDPOINT_USERNAME, password=MONGO_ENDPOINT_PASSWORD)
    documents_c = client[db_name].items
    

    BATCH = 10000
    # Find the document with the maximum popularity value
    max_popularity_doc = documents_c.find_one(sort=[("popularity", -1)])

    # Check if there's a result
    if max_popularity_doc:
        max_popularity = max_popularity_doc["popularity"]
        print(f"The maximum popularity is: {max_popularity}")
    else:
        raise Exception("No documents found in the collection or popularity field is missing.")

    es = Elasticsearch(
                hosts=f'https://{ELASTIC_ENDPOINT}:{ELASTIC_PORT}',
                request_timeout=60,
                max_retries=10, 
                retry_on_timeout=True,
                basic_auth=(ELASTIC_USER, ELASTIC_PW),
                ssl_assert_fingerprint=CERT_FINGERPRINT
            )


    
    resp = es.options(ignore_status=[400]).indices.create(
        index="wikidata",
        settings=MAPPING["settings"],
        mappings=MAPPING["mappings"],
    )


    TOTAL_DOCS = documents_c.estimated_document_count()
    results = documents_c.find({})

    buffer = []
    index = 0
    for i, item in enumerate(tqdm(results, total=TOTAL_DOCS)):
        id_entity = item["entity"]
        names = list((item["labels"].values()))
        aliases = [alias for lang in item["aliases"] for alias in lang]
        names = list(set(names + aliases))
        description = ""
        if "value" in item["description"]:
            description = item["description"].get("value", "")
        

        NERtype = item["NERtype"]
        types = item["types"] 
        kind = item["kind"] 
        popularity = int(item["popularity"])
        
        
        if NERtype == "PERS":
            name = item["labels"].get("en")
            if name is not None:
                name_abbrevations = generate_dot_notation_options(name)
                names = list(set(names + name_abbrevations))


        for j, name in enumerate(names):
            doc = {
                "_op_type":"index",
                "_index": "wikidata",
                "_id": index,
                "id": id_entity,
                "name": name,
                "description": description,
                "kind":  kind,
                "NERtype":  NERtype,
                "types":  " ".join(types["P31"]),
                "length": len(name),
                "ntoken": len(name.split(' ')),
                "popularity": round(popularity / max_popularity, 2)
            }
            
            index += 1 
            buffer.append(doc)

            if len(buffer) >= BATCH:
                index_documents(es, buffer)
                buffer = []
                

    if len(buffer) > 0:
        index_documents(es, buffer)
        buffer = []

    print('All Finished') 
except Exception as e:
    print(e)
    traceback.print_exc()  # This will print the full traceback
    print("An error occurred. Exiting...")
    sys.exit(1)