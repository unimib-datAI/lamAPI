from model.elastic import Elastic
from model.utils import editdistance, clean_str, compute_similarity_between_string
import datetime

class LookupRetriever:

    def __init__(self, database):
        self.database = database
        #self.candidate_cache_collection.create_index([('cell', 1), ('type', 1), ('kg', 1), ('size', 1)], unique=True)
        self.elastic_retriever = Elastic()

    def search(self, label, limit = 100, kg = "wikidata", fuzzy = False, types = None, ids = None):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)
        label_norm = label.strip().lower()   
        query_result = self._exec_query(label_norm, limit = limit, kg = kg,
                                        fuzzy = fuzzy, types = types, ids = ids)
            
        return query_result

    def _exec_query(self, label, limit=100, kg = "wikidata", fuzzy = False, types = None, ids = None):
        
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

        body = self.create_query(name = label, fuzzy = fuzzy, types = types)
        history = {}
        final_result = {label: []}
        result = []
    
        result, _ = self.elastic_retriever.search(body, kg, limit)

        if ids is not None:
            body = self.create_ids_query(name = label, ids=ids)
            result2, _ = self.elastic_retriever.search(body, kg, 500)
            result = result + result2
        
        mention_clean = clean_str(label)
        ntoken_mention = len(label.split(" "))
        length_mention = len(label)
   
        

        body = self.create_token_query(name=label)
        result_to_discard, _ = self.elastic_retriever.search(body, kg, limit)
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
        
        ids = list(set([t for entity in result for t in entity["types"].split(" ")]))
        items_collection = self.database.get_requested_collection("items", kg=kg)        
        results = items_collection.find({"category": "type", "entity": {"$in": ids}})
        types_id_to_name = {result["entity"]:result["labels"].get("en") for result in results}

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
            if id_entity not in history:
                history[id_entity] = obj
            elif (ed_score+jaccard_score) > (history[id_entity]["ed_score"]+history[id_entity]["jaccard_score"]):
                history[id_entity] = obj

            
        final_result[label] = list(history.values())
    
       
        try:
            self.candidate_cache_collection.insert_one({
                "cell": label,
                "type": types,
                "kg": kg,
                "candidates": final_result[label],
                "lastAccessed": datetime.datetime.utcnow(),
                "fuzzy": fuzzy,
                "limit": limit,
                "query": body
            })
        except:
            pass    

        return final_result

    
    def _exec_multi_query(self, labels, limit=100, kg = "wikidata", fuzzy = False, ngrams = False, types = None, ids = None):
        if types is not None:
            types = types.split(" ").sort()
       
        body = {"cell": {"$in": labels}, "type": types, "kg": kg, "fuzzy": fuzzy, "ngrams": ngrams, "limit": limit}
        result = self.candidate_cache_collection.find(body)
        final_result = {}
        cell_buffer = []
        ids = []
        if result is not None:
            for item in result:
                final_result[item["cell"]] = item["candidates"]
                cell_buffer.append(item["cell"])
                ids.append(item["_id"])
        
        self.candidate_cache_collection.update_many(
            {"_id": {"$in": ids}},
            {"$set": { "lastAccessed": datetime.datetime.utcnow() }}
        )

        missing_cells = set(labels) - set(cell_buffer)
        for cell in missing_cells:
            query_result = self._exec_query(cell, limit = limit, kg = kg, fuzzy = fuzzy, ngrams = ngrams, types = types)
            final_result[cell] = query_result[cell]

        return final_result

    def create_query(self, name, fuzzy=False, types=None):
        splitted_name = name.split(" ")
        
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
        query_base["query"]["bool"]["must"].append({"range": {"ntoken": {"gte": len(splitted_name) - 3, "lte": len(splitted_name) + 3}}})
        
        # add token
        query_base["query"]["bool"]["should"].append({"match": {"name": {"query": name, "boost": 2}}})

        # add fuzzy
        if fuzzy:
            query_base["query"]["bool"]["should"].append({"match": {"name": {"query": name, "fuzziness": "auto"}}})

        # add types constraint
        if types is not None:
            query_base["query"]["bool"]["should"].append({"match": {"type": types}})
        
        return query_base


    def create_token_query(self, name):
        query = {"query":{"match":{"name": name}}}

        return query