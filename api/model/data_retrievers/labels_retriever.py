class LabelsRetriever:
    def __init__(self, database):
        self.database = database

    def get_labels(self, entities=None, kg="wikidata", category=None):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"entity": {"$in": entities}}
        if category is not None:
            query["category"] = category
        
        return self.database.get_requested_collection("items", kg).find(query)

    def get_labels_output(self, entities=None, kg="wikidata", lang=None, category=None):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        final_result = {}
        retrieved_data = self.get_labels(entities, kg, category)
        
        for obj in retrieved_data:
            entity_id = obj["entity"]
            entity_info = {
                "kind": obj.get("kind", None),
                "NERtype": obj.get("NERtype", None),
                "url": self.database.get_url_kgs().get(kg, "") + entity_id,
                "description": obj.get("description", {}).get("value"),
                "labels": obj.get("labels", {}),
                "aliases": obj.get("aliases", {})
            }
            
            if lang:
                if lang in obj.get("labels", {}):
                    entity_info["labels"] = {lang: obj["labels"][lang]}
                if lang in obj.get("aliases", {}):
                    entity_info["aliases"] = {lang: obj["aliases"][lang]}
            
            final_result[entity_id] = entity_info
        
        return final_result