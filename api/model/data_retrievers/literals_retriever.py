class LiteralsRetriever:
    def __init__(self, database):
        self.database = database

    def get_literals(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")

        query = {"entity": {"$in": entities}}
        return self.database.get_requested_collection("literals", kg).find(query)

    def get_literals_output(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")

        final_response = {}
        wiki_objects_retrieved = self.get_literals(entities=entities, kg=kg)

        for entity_type in wiki_objects_retrieved:
            entity_id = entity_type["entity"]
            entity_types = entity_type.get("literals", [])

            final_response[entity_id] = {"literals": entity_types}

        return final_response
