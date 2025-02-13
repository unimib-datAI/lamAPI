class SummaryRetriever:
    def __init__(self, database):
        self.database = database

    def get_summary(self, data_type, entities=None, kg="wikidata", rank_order="desc", k=10):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        collection_name = f"{data_type}Summary"
        collection = self.database.get_requested_collection(collection_name, kg)

        # Basic query to retrieve all or specific entities; adjust based on actual data schema
        query = {"entity": {"$in": entities}} if entities else {}

        # Apply ordering and limit if specified
        sort_order = -1 if rank_order == "desc" else 1
        cursor = collection.find(query, {"_id": 0}).sort("count", sort_order).limit(k)
        
        return list(cursor)

    def get_objects_summary(self, entities=None, kg="wikidata", rank_order="desc", k=10):
        return self.get_summary("objects", entities, kg, rank_order, k)

    def get_literals_summary(self, entities=None, kg="wikidata", rank_order="desc", k=10):
        return self.get_summary("literals", entities, kg, rank_order, k)