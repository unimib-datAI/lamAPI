import os
import time
import subprocess
import json
import sys
import traceback

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from pymongo import MongoClient
from tqdm import tqdm

from conf import MAPPING


def index_documents(es, buffer, max_retries=5):
    for _ in range(max_retries):
        try:
            bulk(es, buffer)
        except Exception as e:
            # Handle the exception or log the error message
            traceback.print_exc()  # This will print the full traceback
            print("An error occurred during indexing:", str(e))
            time.sleep(5)
            continue
    else:
        print("Max retries exceeded. Failed to index the documents.")

        
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
    kg_name = sys.argv[1:][1]
except:
    sys.exit("Please provide a DB name and KG name as arguments")

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



    with open("../index_mappings.json") as f:
        index_mappings = json.loads(f.read())[kg_name]["indexes"]


    for cluster in index_mappings:
        index_name = index_mappings[cluster]
        resp = es.options(ignore_status=[400]).indices.create(
            index=index_name,
            settings=MAPPING["settings"],
            mappings=MAPPING["mappings"],
        )


    TOTAL_DOCS = documents_c.estimated_document_count()
    results = documents_c.find({})

    human_set = set({"Q5"})
    disambiguation_set = set({"Q4167410"})
    category_set = set({"Q4167836"})
    species_event_timeinterval_physicalobject_set = set({"Q1656682", "Q186081", "Q223557", "Q7432"})
    creativework_film_videogame_tvseries_set = set({"Q17537576", "Q7889", "Q229390", "Q261636"})
    organization_set = set({"Q43229", "Q891723", "Q18388277", "Q1058914",
                        "Q161726", "Q6881511", "Q4830453", "Q431289", "Q891723", 
                        "Q167037", "Q18388277", "Q1055701"})
    location_set = set({
        "Q2221906", "Q3624078", "Q619610", "Q179164", "Q7270", 
        "Q51576574", "Q185145", "Q113489728", "Q1520223", 
        "Q5255892", "Q512187", "Q99541706", "Q1200957", "Q1637706", "Q5119",
        "Q208511", "Q174844", "Q51929311", "Q1187811", "Q1549591"})


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
        
            
        types = item["types"] 
        category = item["category"] 
        popularity = int(item["popularity"])
        
        if "Q5" in types["P31"]:
            name = item["labels"].get("en")
            if name is not None:
                name_abbrevations = generate_dot_notation_options(name)
                names = list(set(names + name_abbrevations))
                
        type_set = set(types["P31"])
        if category == "predicate":
            index_name = index_mappings["predicate"]
        elif category == "type":    
            index_name = index_mappings["type"]
        elif len(type_set.intersection(human_set)) > 0:
            index_name = index_mappings["human"]
        elif len(type_set.intersection(disambiguation_set)) > 0:
            index_name = index_mappings["disambiguation"]    
        elif len(type_set.intersection(category_set)) > 0:
            index_name = index_mappings["category"]
        elif len(type_set.intersection(species_event_timeinterval_physicalobject_set)) > 0:
            index_name = index_mappings["species_event_timeinterval_physicalobject"]     
        elif len(type_set.intersection(creativework_film_videogame_tvseries_set)) > 0:
            index_name = index_mappings["creativework_film_videogame_tvseries"]
        elif len(type_set.intersection(organization_set)) > 0:
            index_name = index_mappings["organization"]     
        elif len(type_set.intersection(location_set)) > 0:
            index_name = index_mappings["location"]
        else:
            index_name = index_mappings["other"]


        for j, name in enumerate(names):
            doc = {
                "_op_type":"index",
                "_index": index_name,
                "_id": index,
                "id": id_entity,
                "name": name,
                "description": description,
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