from model.utils import build_error
from model.utils import recognize_entity


class PredicatesRetriever:

    def __init__(self, database):
        self.database = database

    def prepare_data(self, entities=[]):
        entity_set = set()
        sub_obj_mapping = {}
        for entity in entities:
            if len(entity) != 2:
                return build_error("error on parsing input data", 400)

            subj = entity[0]
            obj = entity[1]
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

    def get_predicates_output(self, entities, kg="wikidata"):
        all_entities, sub_obj_mapping = self.prepare_data(entities)

        entity_objects = {}

        if kg in self.database.get_supported_kgs():
            entity_objects = self.database.get_requested_collection("objects", kg).get_objects(all_entities, kg)

        final_response = {}

        if kg in self.database.get_supported_kgs():
            wiki_response = {}
            for subj in sub_obj_mapping:
                if subj in entity_objects:
                    for current_obj in sub_obj_mapping[subj]:
                        if current_obj in entity_objects[subj]["objects"].keys():
                            wiki_response[f"{subj} {current_obj}"] = entity_objects[subj]["objects"][current_obj]

            final_response[kg] = wiki_response

        return final_response
