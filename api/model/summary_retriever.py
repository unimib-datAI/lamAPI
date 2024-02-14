class SummaryRetriever:
    def __init__(self, database):
        self.database = database

    def get_summary(self, data_type, entities=[], kg="wikidata", rank_order=None, k=10):
        if kg in self.database.get_supported_kgs():
            collection_name = f"{data_type}Summary"
            collection = self.database.get_requested_collection(collection_name, kg)
            
            # Basic query to retrieve all or specific entities; adjust based on actual data schema
            query = {'entity': {'$in': entities}} if entities else {}
            
            # Apply ordering and limit if specified
            cursor = collection.find(query, {"_id": 0})
            print("result", list(cursor), flush=True)
            if rank_order:
                cursor = cursor.sort("count", -1 if rank_order == "desc" else 1)
            if k:
                cursor = cursor.limit(k)
        collection = self.database.get_requested_collection(collection_name, kg)
        cursor = collection.find(query, {"_id": 0})
        print("result", list(cursor), flush=True)        
        return list(cursor)

    def get_objects_summary(self, entities=[], kg="wikidata", rank_order='desc', k=10):
        return self.get_summary("objects", entities, kg, rank_order, k)

    def get_literals_summary(self, entities=[], kg="wikidata", rank_order='desc', k=10):
        return self.get_summary("literals", entities, kg, rank_order, k)
