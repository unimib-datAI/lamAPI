import base64

class BOWRetriever:

    def __init__(self, database):
        self.database = database

    def get_bow_from_db(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"id": {"$in": entities}}
        return self.database.get_requested_collection("items_vectors", kg).find(query)

    def get_bow(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        
        entity_bow = {}
        items_retrieved = self.get_bow_from_db(entities=entities, kg=kg)
        
        for item in items_retrieved:
            entity_id = item["id"]
            bow = item.get("bow", [])
            entity_bow[entity_id] = base64.b64encode(bow).decode('utf-8')

        return entity_bow

    def get_bow_output(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        return self.get_bow(entities, kg=kg)