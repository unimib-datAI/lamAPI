class SameasRetriever:

    def __init__(self, database):
        self.database = database

    def get_sameas(self, entities=[], kg="wikidata"):
        return self.database.get_requested_collection("mappings", kg).find({"wikidata_id": {"$in": list(entities)}})

    def get_sameas_output(self, entities=[], kg="wikidata"):
        final_response = {}

        sameas_retrieved = self.get_sameas(entities=entities, kg=kg)
        wiki_entity_objects = {}
        for item in sameas_retrieved:
            entity_id = item["wikidata_id"]
            wiki_entity_objects[entity_id] = {}
            wiki_entity_objects[entity_id]["wikidata"] = f"http://wikidata.org/entity/{entity_id}"
            wiki_entity_objects[entity_id]["wikipedia"] = f"http://en.wikipedia.org/wiki/{item['wikipedia_id']}"
            wiki_entity_objects[entity_id]["dbpedia"] = f"http://dbpedia.org/resource/{item['dbpedia_id']}"

        final_response = wiki_entity_objects

        return final_response
