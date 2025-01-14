class ObjectsRetriever:
    def __init__(self, database):
        self.database = database

    def get_objects_from_db(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"entity": {"$in": entities}}
        return self.database.get_requested_collection("objects", kg).find(query)

    def get_objects(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        
        entity_objects = {}
        objects_retrieved = self.get_objects_from_db(entities=entities, kg=kg)
        
        for entity_type in objects_retrieved:
            entity_id = entity_type["entity"]
            entity_types = entity_type.get("objects", [])
            entity_objects[entity_id] = {"objects": entity_types}

        return entity_objects

    def get_objects_output(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        return self.get_objects(entities, kg=kg)
