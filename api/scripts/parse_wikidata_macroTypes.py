import bz2
import json
from tqdm import tqdm
import traceback
import os
from pymongo import MongoClient
from pymongo import *
from pymongo import errors
import configparser
from json.decoder import JSONDecodeError

############################################

# docker compose -f docker-compose-dev.yml up -d

############################################

# MongoDB connection setup
MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_ENDPOINT_PORT = int(MONGO_ENDPOINT_PORT)
MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
DB_NAME = f"wikidata"

client = MongoClient(MONGO_ENDPOINT, MONGO_ENDPOINT_PORT, username=MONGO_ENDPOINT_USERNAME, password=MONGO_ENDPOINT_PASSWORD)
client

from requests import get

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


wikidata_dump_path = './my-data/latest-all.json.bz2'

DATATYPES_MAPPINGS = {
    'external-id': 'STRING',
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

try:
    geolocation_subclass = get_wikidata_item_tree_item_idsSPARQL([2221906], backward_properties=[279])
    food_subclass =  get_wikidata_item_tree_item_idsSPARQL([2095], backward_properties=[279])
    edInst_subclass =  get_wikidata_item_tree_item_idsSPARQL([2385804], backward_properties=[279])
    govAgency_subclass =  get_wikidata_item_tree_item_idsSPARQL([327333], backward_properties=[279])
    intOrg_subclass =  get_wikidata_item_tree_item_idsSPARQL([484652], backward_properties=[279])
    timeZone_subclass =  get_wikidata_item_tree_item_idsSPARQL([12143], backward_properties=[279])    
    geolocation_subclass = list(set(geolocation_subclass)-set(food_subclass)-set(edInst_subclass)-set(govAgency_subclass)-
                            set(intOrg_subclass)-set(timeZone_subclass))
    
    organization_subclass=get_wikidata_item_tree_item_idsSPARQL([43229], backward_properties=[279])    
    country_subclass =  get_wikidata_item_tree_item_idsSPARQL([6256], backward_properties=[279])    
    city_subclass =  get_wikidata_item_tree_item_idsSPARQL([515], backward_properties=[279])    
    capitals_subclass =  get_wikidata_item_tree_item_idsSPARQL([5119], backward_properties=[279])

    admTerr_subclass =  get_wikidata_item_tree_item_idsSPARQL([15916867], backward_properties=[279])

    family_subclass =  get_wikidata_item_tree_item_idsSPARQL([17350442], backward_properties=[279])
    sportLeague_subclass =  get_wikidata_item_tree_item_idsSPARQL([623109], backward_properties=[279])
    venue_subclass =  get_wikidata_item_tree_item_idsSPARQL([8436], backward_properties=[279])
    organization_subclass = list(set(organization_subclass)-set(country_subclass)-set(city_subclass)-
                             set(capitals_subclass)-set(admTerr_subclass)-set(family_subclass) -
                            set(sportLeague_subclass)-set(venue_subclass))
    
except json.decoder.JSONDecodeError:
    pass

with bz2.open(wikidata_dump_path, 'rt', encoding='utf-8') as f:
    count = 0
    
    ORG = []
    PERS = []
    LOC = []
    OTHERS = []
             
    for i, line in tqdm(enumerate(f), total=1000):
        if count == 10000:
            break
        try:
            count += 1
            # Parse JSON data from each line
            data = json.loads(line[:-2])

            entity = data['id']
            labels = data.get("labels", {})
            english_label = labels.get("en", {}).get("value", "")
            aliases = data.get("aliases", {})
            description = data.get('descriptions', {}).get('en', {}).get("value", "")
            category = "entity"
            sitelinks = data.get("sitelinks", {})
            popularity = len(sitelinks) if len(sitelinks) > 0 else 1


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

            
            if data.get("type") == "item" and "claims" in data:
                p31_claims = data["claims"].get("P31", [])
                for claim in p31_claims:
                    mainsnak = claim.get("mainsnak", {})
                    datavalue = mainsnak.get("datavalue", {})
                    numeric_id = datavalue.get("value", {}).get("numeric-id")
                    if numeric_id in organization_subclass:
                        ORG.append(numeric_id)

                    elif numeric_id == 5:
                        PERS.append(numeric_id)
                        
                    elif numeric_id in geolocation_subclass:
                        LOC.append(numeric_id)
                        
                    else:
                        OTHERS.append(numeric_id)
                        
                    
                    
            ################################################################   


            ################################################################   
            # URL EXTRACTION

            wikidata_dump_path = './my-data/latest-all.json.bz2'

            with bz2.open(wikidata_dump_path, 'rt', encoding='utf-8') as f:
                count = 0
                
                        
                for i, line in tqdm(enumerate(f), total=1000):
                    if count == 10000:
                        break
                    try:
                        count += 1
                        # Parse JSON data from each line
                        data = json.loads(line[:-2])
                    
                        labels = data.get("labels", {})
                        lang = labels.get("en", {}).get("language", "")
                        entry={}
                        entry["WD_id"] = data['id']
                        entry["WP_id"] = labels.get("en", {}).get("value", "")

                        entry["WD_id_URL"] = "http://www.wikidata.org/wiki/"+entry["WD_id"]
                        entry["WP_id_URL"] = "http://"+lang+".wikipedia.org/wiki/"+sitelinks['enwiki']['title'].replace(' ','_')
                        entry["dbpedia_URL"] = "http://dbpedia.org/resource/"+sitelinks['enwiki']['title'].replace(' ','_')
                        
                        print("------------------")
                        print(entry["WD_id_URL"])
                        print(entry["WP_id_URL"])
                        print(entry["dbpedia_URL"])
                        print("------------------")
                
                    except json.decoder.JSONDecodeError:
                        continue
            
            ################################################################    
            
           
            
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
            for predicate in data["claims"]:
                if predicate == "P279":
                    found = True

            if found:
                category = "type"
            if entity[0] == "P":
                category = "predicate"

            objects = {}
            literals = {datatype: {} for datatype in DATATYPES}
            types = {"P31": []}
            
            
            predicates = data["claims"]
            for predicate in predicates:
                for obj in predicates[predicate]:
                    datatype = obj["mainsnak"]["datatype"]

                    if check_skip(obj, datatype):
                        continue

                    # here you have just the Q.....
                    if datatype == "wikibase-item" or datatype == "wikibase-property":
                        value = obj["mainsnak"]["datavalue"]["value"]["id"]

                        if predicate == "P31" or predicate == "P106":
                            types["P31"].append(value)

                        if value not in objects:
                            objects[value] = []
                        objects[value].append(predicate)
                        #print(f"value_pred (item/property): {value}")
                    # here you have numbers or letters
                    else:
                        value = get_value(obj, datatype)
                        lit = literals[DATATYPES_MAPPINGS[datatype]]

                        if predicate not in lit:
                            lit[predicate] = []
                            
                        #print(f"value_pred(other): {value}")
                        lit[predicate].append(value)
                        
        except json.decoder.JSONDecodeError:
            continue


total_length_PERS = len(PERS)
total_length_ORG = len(ORG)
total_length_LOC = len(LOC)
total_length_OTHERS = len(OTHERS)

# Print the total lengths
print("Total lengths:")
print(f"Length of PERS: {total_length_PERS}")
print(f"Length of ORG: {total_length_ORG}")
print(f"Length of LOC: {total_length_LOC}")
print(f"Length of OTHERS: {total_length_OTHERS}")

# Calculate the sum of lengths
total_length = total_length_PERS + total_length_ORG + total_length_LOC + total_length_OTHERS

# Print the sum of lengths
print(f"Total length: {total_length}")