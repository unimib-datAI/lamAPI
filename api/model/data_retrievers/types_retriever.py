class TypesRetriever:
    def __init__(self, database):
        self.database = database

    def get_types(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"entity": {"$in": entities}}
        return self.database.get_requested_collection("types", kg).find(query)

    def get_types_output(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        final_response = {}
        wiki_types_retrieved = self.get_types(entities=entities, kg=kg)
        
        for entity_type in wiki_types_retrieved:
            entity_id = entity_type["entity"]
            entity_types = entity_type.get("types", [])
            final_response[entity_id] = {"types": entity_types}

        return final_response