class TypesRetriever:

    def __init__(self, database):
        self.database = database

    def get_types(self, entities=[], kg="wikidata"):
        if kg in self.database.get_supported_kgs():
            return self.database.get_requested_collection("types", kg).find({"entity": {"$in": list(entities)}})

    def get_types_output(self, entities=[], kg=[]):

        final_response = {}

        if kg in self.database.get_supported_kgs():
            wiki_types_retrieved = self.get_types(entities=entities, kg=kg)
            wiki_entity_types = {}
            for entity_type in wiki_types_retrieved:
                entity_id = entity_type["entity"]
                entity_types = entity_type["types"]

                wiki_entity_types[entity_id] = {}
                wiki_entity_types[entity_id]["types"] = entity_types

            final_response["wikidata"] = wiki_entity_types

        return final_response
