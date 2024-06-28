import os
from pymongo import MongoClient
from datetime import datetime

# Constants
MONGO_ENDPOINT, MONGO_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
SUPPORTED_KGS = os.environ["SUPPORTED_KGS"]
SUPPORTED_KGS = SUPPORTED_KGS.split(",")


class Database:

    def __init__(self):
        self.mongo = MongoClient(MONGO_ENDPOINT, int(MONGO_PORT), username=MONGO_USERNAME, password=MONGO_PASSWORD)
        self.mappings = {kg.lower(): None for kg in SUPPORTED_KGS}
        self.update_mappings()
        self.create_indexes()

    def update_mappings(self):
        history = {}
        for db in self.mongo.list_database_names():
            # Handle real databases
            doc = self.mongo[db]["metadata"].find_one()
            if doc is not None and doc.get("status") == "DOING":
                continue
            kg_name = "".join(filter(str.isalpha, db))
            date = "".join(filter(str.isdigit, db))
            if kg_name in self.mappings:  # Exclude the fake database
                parsed_date = datetime.strptime(date, "%d%m%Y")
                if kg_name not in history:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db
                elif parsed_date > history[kg_name]:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db

    def create_indexes(self):
        print("Creating indexes...", flush=True)
        # Specify the collections and their respective fields to be indexed
        index_specs = {
            "cache": ["name", "lastAccessed", "limit"],  # Example: Indexing 'name' and 'lastAccessed' fields in 'cache' collection
            "items": ["id_entity", "entity", "category", "popularity"],
            "literals": ["id_entity", "entity"],
            "objects": ["id_entity", "entity"],
            "types": ["id_entity", "entity"],
        }
        print("mappings", self.mappings, flush=True)
        for db_name in self.mappings.values():
            db = self.mongo[db_name]
            for collection, fields in index_specs.items():
                if collection == "cache":
                    db[collection].create_index(
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
                        background=True,  # Create the index in the background
                    )
                elif collection == "items":
                    db[collection].create_index(
                        [("entity", 1), ("kind", 1)],
                        unique=True,
                        background=True,  # Create the index in the background
                    )
                for field in fields:
                    db[collection].create_index([(field, 1)], background=True)  # 1 for ascending order, background indexing
        print("Indexes created.", flush=True)

    def get_supported_kgs(self):
        return self.mappings

    def get_url_kgs(self):  # hard-coded for now
        return {"wikidata": "https://www.wikidata.org/wiki/", "crunchbase": "https://www.crunchbase.com/organization/"}

    def get_requested_collection(self, collection, kg="wikidata"):
        self.update_mappings()
        print(f"KG: {kg}", collection, self.mappings, flush=True)
        if kg in self.mappings and self.mappings[kg] is not None:
            return self.mongo[self.mappings[kg]][collection]
        else:
            raise ValueError(f"KG {kg} is not supported.")
