import bz2
import json
import os
import sys
import traceback
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime
from requests import get


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
            
def get_wikidata_item_tree_item_idsSPARQL(root_items, forward_properties=None, backward_properties=None):
    """Return ids of WikiData items, which are in the tree spanned by the given root items and claims relating them
        to other items.
    --------------------------------------------
    For example, if you have an item with types A, B, and C, and you specify a forward property that applies to type B, the item will 
    be included in the result because it has type B, even if it also has types A and C
    --------------------------------------------  
    :param root_items: iterable[int] One or multiple item entities that are the root elements of the tree
    :param forward_properties: iterable[int] | None property-claims to follow forward; that is, if root item R has
        a claim P:I, and P is in the list, the search will branch recursively to item I as well.
    :param backward_properties: iterable[int] | None property-claims to follow in reverse; that is, if (for a root
        item R) an item I has a claim P:R, and P is in the list, the search will branch recursively to item I as well.
    :return: iterable[int]: List with ids of WikiData items in the tree
    """

    query = '''PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>'''
    if forward_properties:
        query +='''SELECT ?WD_id WHERE {
                  ?tree0 (wdt:P%s)* ?WD_id .
                  BIND (wd:%s AS ?tree0)
                  }'''%( ','.join(map(str, forward_properties)),','.join(map(str, root_items)))
    elif backward_properties:
        query+='''SELECT ?WD_id WHERE {
                    ?WD_id (wdt:P%s)* wd:Q%s .
                    }'''%(','.join(map(str, backward_properties)), ','.join(map(str, root_items)))
    #print(query)

    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    data = get(url, params={'query': query, 'format': 'json'}).json()
    
    ids = []
    for item in data['results']['bindings']:
        this_id=item["WD_id"]["value"].split("/")[-1].lstrip("Q")
        #print(item)
        try:
            this_id = int(this_id)
            ids.append(this_id)
            #print(this_id)
        except ValueError:
            #print("exception")
            continue
    return ids
            
def parse_data(item, i, geolocation_subclass, organization_subclass):
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

    ###############################################################
    # ORGANIZATION EXTRACTION
    # All items with the root class Organization (Q43229) excluding country (Q6256), city (Q515), capitals (Q5119), 
    # administrative territorial entity of a single country (Q15916867), venue (Q17350442), sports league (Q623109) 
    # and family (Q8436)
    
    # LOCATION EXTRACTION
    # All items with the root class Geographic Location (Q2221906) excluding: food (Q2095), educational institution (Q2385804), 
    # government agency (Q327333), international organization (Q484652) and time zone (Q12143)
    
    # PERSON EXTRACTION
    # All items with the statement is instance of (P31) human (Q5) are classiﬁed as person.

    NERtype = None

    if item.get("type") == "item" and "claims" in item:
        p31_claims = item["claims"].get("P31", [])
        
        if len(p31_claims) != 0:           
            for claim in p31_claims:
                mainsnak = claim.get("mainsnak", {})
                datavalue = mainsnak.get("datavalue", {})
                numeric_id = datavalue.get("value", {}).get("numeric-id")
                
                if numeric_id == 5:
                    NERtype = "PERS" 
                elif numeric_id in geolocation_subclass or any(k.lower() in description.get('value', '').lower() for k in ["district", "city", "country", "capital"]):
                    NERtype = "LOC"
                elif numeric_id in organization_subclass:
                    NERtype = "ORG"  
                else:
                    NERtype = "OTHERS"
        else:
            NERtype = "OTHERS"  
                        
    ################################################################

    ################################################################   
    # URL EXTRACTION

    try:
        lang = labels.get("en", {}).get("language", "")
        tmp={}
        tmp["WD_id"] = item['id']
        tmp["WP_id"] = labels.get("en", {}).get("value", "")

        url_dict={}
        url_dict["wikidata"] = "http://www.wikidata.org/wiki/"+url_dict["WD_id"]
        url_dict["wikipedia"] = "http://"+lang+".wikipedia.org/wiki/"+url_dict["WP_id"].replace(" ","_")
        url_dict["dbpedia"] = "http://dbpedia.org/resource/"+url_dict["WP_id"].capitalize().replace(" ","_")
        

    except json.decoder.JSONDecodeError:
       pass
    
    ################################################################    



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
            "kind": category,   # kind (entity, type or predicate, disambiguation or category)
            ######################
            # new updates
            "NERtype": NERtype, # (ORG, LOC, PER or OTHERS)
            "URLs" : url_dict
            ######################
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

    try:
        organization_subclass = get_wikidata_item_tree_item_idsSPARQL([43229], backward_properties=[279])
        #print(len(organization_subclass))
    except json.decoder.JSONDecodeError:
        pass
    
    try:
        country_subclass = get_wikidata_item_tree_item_idsSPARQL([6256], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        country_subclass = set()
        pass
    
    try:
        city_subclass = get_wikidata_item_tree_item_idsSPARQL([515], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        city_subclass = set()
        pass
    
    try:
        capitals_subclass = get_wikidata_item_tree_item_idsSPARQL([5119], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        capitals_subclass = set()
        pass
    
    try:
        admTerr_subclass = get_wikidata_item_tree_item_idsSPARQL([15916867], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        admTerr_subclass = set()
        pass
    
    try:
        family_subclass = get_wikidata_item_tree_item_idsSPARQL([17350442], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        family_subclass = set()
        pass
    
    try:
        sportLeague_subclass = get_wikidata_item_tree_item_idsSPARQL([623109], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        sportLeague_subclass = set()
        pass
    
    try:
        venue_subclass = get_wikidata_item_tree_item_idsSPARQL([8436], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        venue_subclass = set()
        pass
        
    try:
        organization_subclass = list(set(organization_subclass) - set(country_subclass) - set(city_subclass) - set(capitals_subclass) - set(admTerr_subclass) - set(family_subclass) - set(sportLeague_subclass) - set(venue_subclass))
        #print(len(organization_subclass))
    except json.decoder.JSONDecodeError:
        pass

    
    try:
        geolocation_subclass = get_wikidata_item_tree_item_idsSPARQL([2221906], backward_properties=[279])
        #print(len(geolocation_subclass))
    except json.decoder.JSONDecodeError:
        pass
    
    try:
        food_subclass = get_wikidata_item_tree_item_idsSPARQL([2095], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        food_subclass = set()
        pass
    
    try:
        edInst_subclass = get_wikidata_item_tree_item_idsSPARQL([2385804], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        edInst_subclass = set()
        pass
    
    try:
        govAgency_subclass = get_wikidata_item_tree_item_idsSPARQL([327333], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        govAgency_subclass = set()
        pass
    
    try:
        intOrg_subclass = get_wikidata_item_tree_item_idsSPARQL([484652], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        intOrg_subclass = set()
        pass
    
    try:
        timeZone_subclass = get_wikidata_item_tree_item_idsSPARQL([12143], backward_properties=[279])
    except json.decoder.JSONDecodeError:
        timeZone_subclass = set()
        pass
       
    try:
        geolocation_subclass = list(set(geolocation_subclass) - set(food_subclass) - set(edInst_subclass) - set(govAgency_subclass) - set(intOrg_subclass) - set(timeZone_subclass))
        #print(len(geolocation_subclass))
    except json.decoder.JSONDecodeError:
        pass

    pbar = tqdm(total=initial_total_lines_estimate)
    for i, line in enumerate(file):
        try:
            item = json.loads(line[:-2])  # Remove the trailing characters
            line_size = len(line)
            current_average_size = update_average_size(line_size)

            # Dynamically update the total based on the current average size
            pbar.total = round(compressed_file_size / current_average_size)
            pbar.update(1)

            parse_data(item, i, geolocation_subclass, organization_subclass)
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
