from model.elastic import Elastic
from model.utils import editdistance, clean_str, compute_similarity_between_string
import datetime
import json

class LookupRetriever:

    def __init__(self, database):
        self.database = database
        #self.candidate_cache_collection.create_index([('cell', 1), ('type', 1), ('kg', 1), ('size', 1)], unique=True)
        self.elastic_retriever = Elastic()

    def search(self, label, limit = 100, kg = "wikidata", fuzzy = False, types = None, ids = None, query = None):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)
        label_norm = label.strip().lower()   
        query_result = self._exec_query(label_norm, limit = limit, kg = kg,
                                        fuzzy = fuzzy, types = types, ids = ids, query = query)
            
        return query_result

    def _exec_query(self, label, limit=100, kg = "wikidata", fuzzy = False, types = None, ids = None, query = None):
        mention_clean = clean_str(label)
        ntoken_mention = len(label.split(" "))
        length_mention = len(label)

        if query is not None:
            query = json.loads(query)
            result, _ = self.elastic_retriever.search(query, kg, limit)
            result = self._get_final_candidates_list(result, mention_clean, kg, None, 
                                                              None, ntoken_mention, length_mention, NER=True)    
            return result

        if types is not None:
            types = types.split(" ")
            types.sort()
            types = " ".join(types)

        body = {"cell": label, "type": types, "kg": kg, "fuzzy": fuzzy, "limit": limit}
        #print("body", body, flush=True)
        result = self.candidate_cache_collection.find_one_and_update(
            body,
            {"$set": { "lastAccessed": datetime.datetime.utcnow() }}
        )
       
        if result is not None:
            final_result = result["candidates"]
            final_result = {label: final_result}
            return final_result

        query = self.create_query(name = label, fuzzy = fuzzy)
        final_result = {label: []}
        result = []
    
        result, _ = self.elastic_retriever.search(query, kg, limit)

        if len(result) == 0:
            query = self.create_query(name = label, fuzzy = True)
            result, _ = self.elastic_retriever.search(query, kg, limit=1000)

        if ids is not None:
            query = self.create_ids_query(name = label, ids=ids)
            result2, _ = self.elastic_retriever.search(query, kg, limit=1000)
            result = result + result2
        
    
        ambiguity_mention, corrects_tokens = self._get_ambiguity_mention(label, mention_clean, kg, limit)
        final_result[label] = self._get_final_candidates_list(result, mention_clean, kg, ambiguity_mention, 
                                                              corrects_tokens, ntoken_mention, length_mention)
       
        try:
            self.candidate_cache_collection.insert_one({
                "cell": label,
                "type": types,
                "kg": kg,
                "candidates": final_result[label],
                "lastAccessed": datetime.datetime.utcnow(),
                "fuzzy": fuzzy,
                "limit": limit,
                "query": query
            })
        except:
            pass    

        return final_result


    def _get_ambiguity_mention(self, label, mention_clean, kg, limit=100):
        query_token = self.create_token_query(name=label)
        result_to_discard, _ = self.elastic_retriever.search(query_token, kg, limit)
        ambiguity_mention, corrects_tokens = (0, 0)
        history_labels, tokens_set = (set(), set())
        for entity in result_to_discard:
            label_clean = clean_str(entity["name"])
            tokens = label_clean.split(" ")
            for token in tokens:
                tokens_set.add(token)
            if mention_clean == label_clean and entity["id"] not in history_labels:
                ambiguity_mention += 1
            history_labels.add(entity["id"])
        tokens_mention = set(mention_clean.split(" "))
        ambiguity_mention = ambiguity_mention / len(history_labels) if len(history_labels) > 0 else 0
        ambiguity_mention = round(ambiguity_mention, 3)
        corrects_tokens = round(len(tokens_mention.intersection(tokens_set)) / len(tokens_mention), 3)
        return ambiguity_mention, corrects_tokens
    
    def _get_final_candidates_list(self, result, mention_clean, kg, ambiguity_mention, corrects_tokens, ntoken_mention, length_mention, NER=False):
        ids = list(set([t for entity in result for t in entity["types"].split(" ")]))
        types_id_to_name = self._get_types_id_to_name(ids, kg)

        history = {}
        for entity in result:
            id_entity = entity["id"]
            label_clean = clean_str(entity["name"])
            ed_score = round(editdistance(label_clean, mention_clean), 2)
            jaccard_score = round(compute_similarity_between_string(label_clean, mention_clean), 2)
            jaccard_ngram_score = round(compute_similarity_between_string(label_clean, mention_clean, 3), 2)
            obj = {
                "id": entity["id"],
                "name": entity["name"],
                "description": entity.get("description", ""),
                "types": [{"id": id_type, "name": types_id_to_name.get(id_type)} for id_type in entity["types"].split(" ")],
                "ambiguity_mention": ambiguity_mention,
                "corrects_tokens": corrects_tokens,
                "ntoken_mention": ntoken_mention,
                "ntoken_entity": entity["ntoken_entity"],
                "length_mention": length_mention,
                "length_entity": entity["length_entity"],
                "popularity": entity["popularity"],
                "pos_score": entity["pos_score"],
                "es_score": entity["es_score"],
                "ed_score": ed_score,
                "jaccard_score": jaccard_score,
                "jaccardNgram_score": jaccard_ngram_score
            }
            if NER:
                obj["kind"] = entity["kind"]
                obj["NERtype"] = entity["NERtype"]

            if id_entity not in history:
                history[id_entity] = obj
            elif (ed_score+jaccard_score) > (history[id_entity]["ed_score"]+history[id_entity]["jaccard_score"]):
                history[id_entity] = obj
                
        return list(history.values())
    
    def _get_types_id_to_name(self, ids, kg):
        items_collection = self.database.get_requested_collection("items", kg=kg)        
        results = items_collection.find({"kind": "type", "entity": {"$in": ids}})
        types_id_to_name = {result["entity"]:result["labels"].get("en") for result in results}
        return types_id_to_name

    def create_token_query(self, name):
        query = {"query":{"match":{"name": name}}}
        return query
    

    def create_ids_query(self, name, ids):
        # base query
        query_base = {
            "query": {
                "bool": {
                    "should": [],
                    "must": []

                }
            },
            "sort": [
                {"popularity": {"order": "desc"}}
            ]
        }

        # add ntoken
        query_base["query"]["bool"]["should"].append({"match": {"name": {"query": name}}})
        query_base["query"]["bool"]["must"].append({"match": {"id": {"query": ids, "boost": 2.0}}})
        return query_base
    
    def create_query(self, name, fuzzy=False):
        splitted_name = name.split(" ")
        
        # base query
        query_base = {
            "query": {
                "bool": {
                    "must": [],
                }
            },
            "sort": [
                {"popularity": {"order": "desc"}}
            ]
        }

        # add ntoken
        query_base["query"]["bool"]["must"].append({"range": {"ntoken": {"gte": len(splitted_name) - 3, "lte": len(splitted_name) + 3}}})
        
        # add fuzzy
        if fuzzy:
            query_base["query"]["bool"]["must"].append({"match": {"name": {"query": name, "fuzziness": "auto"}}})
        else:
            query_base["query"]["bool"]["must"].append({"match": {"name": {"query": name, "boost": 2}}}) # add token (normal query)
        
        return query_base
