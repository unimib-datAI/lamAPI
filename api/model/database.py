import os
from pymongo import MongoClient
from datetime import datetime

# Constants
MONGO_ENDPOINT, MONGO_PORT = os.environ["MONGO_ENDPOINT"].split(":")
SUPPORTED_KGS = os.environ["SUPPORTED_KGS"]
SUPPORTED_KGS = SUPPORTED_KGS.split(",")


class Database:

    def __init__(self):
        self.mongo = MongoClient(MONGO_ENDPOINT, int(MONGO_PORT))
        self.mappings = {kg.lower(): None for kg in SUPPORTED_KGS}
        self.update_mappings()

    def update_mappings(self):
        history = {}
        for db in self.mongo.list_database_names():
            # Handle real databases
            doc = self.mongo[db]["metadata"].find_one()
            if doc is not None and doc.get("status") == "DOING":
                continue
            kg_name = "".join(filter(str.isalpha, db))
            date = "".join(filter(str.isdigit, db))
            if kg_name in self.mappings:  
                parsed_date = datetime.now()
                if date != "":
                    parsed_date = datetime.strptime(date, "%d%m%Y")
                if kg_name not in history:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db
                elif parsed_date > history[kg_name]:
                    history[kg_name] = parsed_date
                    self.mappings[kg_name] = db

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
