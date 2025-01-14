from model.utils import build_error, recognize_entity


class PredicatesRetriever:
    def __init__(self, database):
        self.database = database

    def prepare_data(self, entities=None):
        if entities is None:
            entities = []
        entity_set = set()
        sub_obj_mapping = {}
        for entity in entities:
            if len(entity) != 2:
                return build_error("error on parsing input data", 400)

            subj, obj = entity
            subj_kg = recognize_entity(subj)
            obj_kg = recognize_entity(obj)
            if subj_kg != obj_kg:
                return build_error("error on parsing input data", 400)
            if subj not in sub_obj_mapping:
                sub_obj_mapping[subj] = [obj]
            else:
                sub_obj_mapping[subj].append(obj)
            entity_set.add(subj)
            entity_set.add(obj)
        return list(entity_set), sub_obj_mapping

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

    def get_predicates_output(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []

        all_entities, sub_obj_mapping = self.prepare_data(entities)
        if isinstance(all_entities, tuple) and all_entities[0] == "error":
            return all_entities  # return error if prepare_data encountered an issue

        entity_objects = self.get_objects(all_entities, kg)

        final_response = {}
        if kg in self.database.get_supported_kgs():
            wiki_response = {}
            for subj in sub_obj_mapping:
                if subj in entity_objects:
                    for current_obj in sub_obj_mapping[subj]:
                        if current_obj in entity_objects[subj]["objects"]:
                            wiki_response[f"{subj} {current_obj}"] = entity_objects[
                                subj
                            ]["objects"][current_obj]

            final_response = wiki_response

        return final_response
