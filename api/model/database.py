import os
import random
from faker import Faker
from pymongo import MongoClient
from datetime import datetime

# Constants
MONGO_ENDPOINT, MONGO_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
SUPPORTED_KGS  = os.environ["SUPPORTED_KGS"]
SUPPORTED_KGS = SUPPORTED_KGS.split(",")
FAKE_DB_NAME = "fake"  # Name of the fake database

class Database():

    def __init__(self):
        self.mongo = MongoClient(
            MONGO_ENDPOINT,
            int(MONGO_PORT), 
            username = MONGO_USERNAME, 
            password = MONGO_PASSWORD
        )
        self.mappings = {kg.lower():None for kg in SUPPORTED_KGS}
        self.mappings["fake"] = FAKE_DB_NAME  # Add the fake database to mappings
        self.initialize_and_populate_fake_db()
        self.update_mappings()

    def update_mappings(self):
        history = {}
        for db in self.mongo.list_database_names():
            # Handle real databases
            doc = self.mongo[db]["metadata"].find_one()
            print(doc)
            if doc is not None and doc.get("status") == "DOING":
                continue
            kg_name = ''.join(filter(str.isalpha, db))
            date = ''.join(filter(str.isdigit, db))
            if kg_name in self.mappings and kg_name != "fake":  # Exclude the fake database
                parsed_date = datetime.strptime(date, "%d%m%Y")
                if kg_name not in history:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db
                elif parsed_date > history[kg_name]:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db
            # Initialize the fake database
            elif kg_name == "fake":
                self.mappings["fake"] = FAKE_DB_NAME

    def initialize_and_populate_fake_db(self):
        if FAKE_DB_NAME not in self.mongo.list_database_names():
            self.populate_fake_db()

            print("Fake database initialized and populated.")

    def populate_fake_db(self):
        fake_db = self.mongo[FAKE_DB_NAME]
        fake = Faker()
        nrandom_exmple = 10000
        # Populating the 'cache' collection
        cache_collection = fake_db["cache"]
        for _ in range(nrandom_exmple):  # Adjust the number for your needs
            word = fake.word()
            cache_data = {
                "cell": word,
                "type": None,
                "kg": "fake",
                "candidates": [{
                    "id": fake.random_number(digits=5),
                    "name": fake.name(),
                    "description": fake.sentence(),
                    "types": [{"id": fake.random_number(digits=5), "name": fake.word()}],
                    "ambiguity_mention": random.uniform(0, 1),
                    "corrects_tokens": random.uniform(0, 1),
                    "ntoken_mention": random.randint(1, 5),
                    "ntoken_entity": random.randint(1, 5),
                    "length_mention": random.randint(1, 10),
                    "length_entity": random.randint(1, 10),
                    "popularity": random.uniform(0, 1),
                    "pos_score": random.uniform(0, 1),
                    "es_score": random.uniform(0, 1),
                    "ed_score": random.uniform(0, 1),
                    "jaccard_score": random.uniform(0, 1),
                    "jaccardNgram_score": random.uniform(0, 1),
                    "cosine_similarity": random.uniform(0, 1)
                } for _ in range(random.randint(1, 100))],  # Random number of candidates
                "lastAccessed": datetime.now(),
                "fuzzy": False,
                "limit": 100,
                "query": {"query": {"match": {"name": word}}}
            }
            cache_collection.insert_one(cache_data)

        # Populating the 'items' collection
        items_collection = fake_db["items"]
        for _ in range(nrandom_exmple):
            items_data = {
                "id_entity": fake.random_number(digits=5),
                "entity": fake.word(),
                "description": {"language": "en", "value": fake.sentence()},
                "labels": {fake.language_code(): fake.word() for _ in range(random.randint(1, 5))},
                "aliases": {fake.language_code(): [fake.word() for _ in range(random.randint(1, 3))] for _ in range(random.randint(1, 5))},
                "types": {"P31": [f'Q{fake.random_number(digits=5)}' for _ in range(random.randint(1, 3))] for _ in range(random.randint(1, 5))},
                "popularity": random.randint(1, 1000),
                "category": "entity"
            }
            items_collection.insert_one(items_data)

        # Populating the 'literals' collection
        literals_collection = fake_db["literals"]
        for _ in range(nrandom_exmple):
            literals_data = {
                "id_entity": fake.random_number(digits=5),
                "entity": fake.word(),
                "literals": {
                    "GEOSHAPE": {"P" + str(fake.random_number(digits=5)): [fake.word()]},
                    "DATETIME": {"P" + str(fake.random_number(digits=5)): [fake.iso8601()]},
                    "MUSICAL_NOTATION": {},
                    "TABULAR_DATA": {},
                    "MATH": {},
                    "NUMBER": {"P" + str(fake.random_number(digits=5)): [str(fake.random_number(digits=8))]},
                    "STRING": {"P" + str(fake.random_number(digits=5)): [fake.word()]}
                }
            }
            literals_collection.insert_one(literals_data)

        # Populating the 'mappings' collection
        mappings_collection = fake_db["mappings"]
        for _ in range(nrandom_exmple):
            mappings_data = {
                "curid": str(fake.random_number(digits=5)),
                "wikipedia_id": fake.word(),
                "wikidata_id": "Q" + str(fake.random_number(digits=5)),
                "dbpedia_id": fake.word()
            }
            mappings_collection.insert_one(mappings_data)

        # Populating the 'objects' collection
        objects_collection = fake_db["objects"]
        for _ in range(nrandom_exmple):
            objects_data = {
                "id_entity": fake.random_number(digits=5),
                "entity": "Q" + str(fake.random_number(digits=5)),
                "objects": {
                    "Q" + str(fake.random_number(digits=5)): ["P" + str(fake.random_number(digits=5))]
                }
            }
            objects_collection.insert_one(objects_data)

        # Populating the 'types' collection
        types_collection = fake_db["types"]
        for _ in range(nrandom_exmple):
            types_data = {
                "id_entity": fake.random_number(digits=5),
                "entity": "Q" + str(fake.random_number(digits=5)),
                "types": {
                    "P" + str(fake.random_number(digits=5)): ["Q" + str(fake.random_number(digits=5))]
                }
            }
            types_collection.insert_one(types_data)

        print("Fake database collections populated.")

    def create_indexes(self):
        # Specify the collections and their respective fields to be indexed
        index_specs = {
            'cache': ['cell', 'lastAccessed'],  # Example: Indexing 'cell' and 'type' fields in 'cache' collection
            'items': ['id_entity', 'entity', 'category', 'popularity'],
            'literals': ['id_entity', 'entity'],
            'mappings': ['curid', 'wikipedia_id', 'wikidata_id', 'dbpedia_id'],
            'objects': ['id_entity', 'entity'],
            'types': ['id_entity', 'entity']
        }

        for db_name in self.mappings.values():
            db = self.mongo[db_name]
            for collection, fields in index_specs.items():
                if collection == "cache":
                    db[collection].create_index([('cell', 1), ('fuzzy', 1), ('type', 1), ('kg', 1), ('limit', 1)], unique=True)
                elif collection == "items":
                    db[collection].create_index([('entity', 1), ('category', 1)], unique=True)    
                for field in fields:
                    db[collection].create_index([(field, 1)])  # 1 for ascending order

    def get_supported_kgs(self):
        return self.mappings

    def get_url_kgs(self): # hard-coded for now
        return {
            "wikidata": "https://www.wikidata.org/wiki/", 
            "crunchbase": "https://www.crunchbase.com/organization/",  
            "fake": "http://fake/"
        }

    def get_requested_collection(self, collection, kg = "wikidata"):
        self.update_mappings()
        if kg in self.mappings and self.mappings[kg] is not None: 
            return self.mongo[self.mappings[kg]][collection]
        else:
            raise ValueError(f"KG {kg} is not supported.")
