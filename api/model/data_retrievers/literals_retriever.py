
class LiteralsRetriever:

    def __init__(self, database):
        self.database = database


    def get_literals(self, entities = [], kg = "wikidata"):
        if kg in self.database.get_supported_kgs():
            return self.database.get_requested_collection("literals", kg).find({'entity': {'$in': list(entities)}})
        
        
    def get_literals_output(self, entities = [], kg = "wikidata"):
        final_response = {}
        if kg in self.database.get_supported_kgs():
            wiki_objects_retrieved = self.get_literals(entities=entities, kg = kg)
            wiki_entity_objects = {}
            for entity_type in wiki_objects_retrieved:
                entity_id = entity_type['entity']
                entity_types = entity_type['literals']

                wiki_entity_objects[entity_id] = {}
                wiki_entity_objects[entity_id]['literals'] = entity_types
            
            final_response['wikidata'] = wiki_entity_objects

        
        return final_response