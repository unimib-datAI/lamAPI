class SameasRetriever:
    def __init__(self, database):
        self.database = database

    def get_sameas(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"entity": {"$in": entities}}
        return self.database.get_requested_collection("items", kg).find(query)

    def get_sameas_output(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        final_response = {}
        sameas_retrieved = self.get_sameas(entities=entities, kg=kg)
        
        for item in sameas_retrieved:
            entity_id = item["entity"]
            final_response[entity_id] = item.get("URLs", [])
        
        return final_response
