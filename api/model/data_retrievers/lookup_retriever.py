from model.elastic import Elastic
from model.utils import editdistance, clean_str, compute_similarity_between_string
import datetime
import json


class LookupRetriever:

    def __init__(self, database):
        self.database = database
        self.elastic_retriever = Elastic()

    def search(
        self,
        name,
        limit=1000,
        kg="wikidata",
        fuzzy=False,
        types=None,
        kind=None,
        NERtype=None,
        language=None,
        ids=None,
        query=None,
        cache=True,
    ):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)
        cleaned_name = clean_str(
            name
        )  # Normalize name to ensure lowercase in order to avoid case-sensitive issues in the cache
        query_result = self._exec_query(
            cleaned_name,
            limit=limit,
            kg=kg,
            fuzzy=fuzzy,
            types=types,
            kind=kind,
            NERtype=NERtype,
            language=language,
            ids=ids,
            query=query,
            cache=cache,
        )
        return query_result

    def _exec_query(self, cleaned_name, limit, kg, fuzzy, types, kind, NERtype, language, ids, query, cache=True):
        self.candidate_cache_collection = self.database.get_requested_collection("cache", kg=kg)

        ntoken_mention = len(cleaned_name.split(" "))
        length_mention = len(cleaned_name)
        ambiguity_mention, corrects_tokens = self._get_ambiguity_mention(cleaned_name, kg, limit)

        if query is not None:
            query = json.loads(query)
            result = self.elastic_retriever.search(query, kg, limit)
            result = self._get_final_candidates_list(
                result, cleaned_name, kg, ambiguity_mention, corrects_tokens, ntoken_mention, length_mention
            )
            return result

        if not cache:
            query = self.create_query(cleaned_name, fuzzy=fuzzy, types=types, kind=kind, NERtype=NERtype, language=language)
            result = self.elastic_retriever.search(query, kg, limit)
            final_result = self._get_final_candidates_list(
                result, cleaned_name, kg, ambiguity_mention, corrects_tokens, ntoken_mention, length_mention
            )
            result = self._check_ids(
                cleaned_name, kg, ids, ntoken_mention, length_mention, ambiguity_mention, corrects_tokens, final_result
            )
            return result

        # Sort types to avoid types duplication in cache due to possible permutations (e.g. "A B" and "B A" are the same type)
        if types is not None:
            types = types.split(" ")
            types.sort()
            types = " ".join(types)

        body = {
            "name": cleaned_name,
            "limit": {"$gte": limit},
            "kg": kg,
            "fuzzy": fuzzy,
            "types": types,
            "kind": kind,
            "NERtype": NERtype,
            "language": language,
        }

        result = self.candidate_cache_collection.find_one_and_update(
            body, {"$set": {"lastAccessed": datetime.datetime.now(datetime.timezone.utc)}}
        )

        if result is not None:
            final_result = result["candidates"][0:limit]
            limit = result["limit"] 
            result = self._check_ids(
                cleaned_name, kg, ids, ntoken_mention, length_mention, ambiguity_mention, corrects_tokens, final_result
            )
            if result is not None:
                final_result = result
                self.add_or_update_cache(body, final_result, limit)
            return final_result

        query = self.create_query(cleaned_name, fuzzy=fuzzy, types=types, kind=kind, NERtype=NERtype, language=language)
        final_result = []

        result = self.elastic_retriever.search(query, kg, limit)
        final_result = self._get_final_candidates_list(
            result, cleaned_name, kg, ambiguity_mention, corrects_tokens, ntoken_mention, length_mention
        )
        result = self._check_ids(
            cleaned_name, kg, ids, ntoken_mention, length_mention, ambiguity_mention, corrects_tokens, final_result
        )
        if result is not None:
            final_result = result
        self.add_or_update_cache(body, final_result, limit)

        return final_result

    def _get_ambiguity_mention(self, cleaned_name, kg, limit=1000):
        query_token = self.create_token_query(name=cleaned_name)
        result_to_discard = self.elastic_retriever.search(query_token, kg, limit)
        ambiguity_mention, corrects_tokens = (0, 0)
        history_labels, tokens_set = (set(), set())
        for entity in result_to_discard:
            label_clean = clean_str(entity["name"])
            tokens = label_clean.split(" ")
            for token in tokens:
                tokens_set.add(token)
            if cleaned_name == label_clean and entity["id"] not in history_labels:
                ambiguity_mention += 1
            history_labels.add(entity["id"])
        tokens_mention = set(cleaned_name.split(" "))
        ambiguity_mention = ambiguity_mention / len(history_labels) if len(history_labels) > 0 else 0
        ambiguity_mention = round(ambiguity_mention, 3)
        corrects_tokens = round(len(tokens_mention.intersection(tokens_set)) / len(tokens_mention), 3)
        return ambiguity_mention, corrects_tokens

    def _get_final_candidates_list(
        self, result, name, kg, ambiguity_mention, corrects_tokens, ntoken_mention, length_mention
    ):
        ids = list(set([t for entity in result for t in entity["types"].split(" ")]))
        types_id_to_name = self._get_types_id_to_name(ids, kg)

        history = {}
        for entity in result:
            id_entity = entity["id"]
            label_clean = clean_str(entity["name"])
            ed_score = round(editdistance(label_clean, name), 2)
            jaccard_score = round(compute_similarity_between_string(label_clean, name), 2)
            jaccard_ngram_score = round(compute_similarity_between_string(label_clean, name, 3), 2)
            obj = {
                "id": entity["id"],
                "name": entity["name"],
                "description": entity.get("description", ""),
                "types": [
                    {"id": id_type, "name": types_id_to_name.get(id_type, id_type)} for id_type in entity["types"].split(" ")
                ],
                "kind": entity.get("kind", None),
                "NERtype": entity.get("NERtype", None),
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
                "jaccardNgram_score": jaccard_ngram_score,
            }
            if id_entity not in history:
                history[id_entity] = obj
            elif (ed_score + jaccard_score) > (history[id_entity]["ed_score"] + history[id_entity]["jaccard_score"]):
                history[id_entity] = obj

        return list(history.values())

    def add_or_update_cache(self, body, final_result, limit):
        """
        Add or update an element in the cache.

        Parameters:
        - body (dict): The query body to identify the cache element.
        - final_result (list): The final result to cache if the element does not exist.
        """
        query = {
            "name": body["name"],
            "limit": limit,
            "kg": body["kg"],
            "fuzzy": body["fuzzy"],
            "types": body.get("types"),
            "kind": body.get("kind"),
            "NERtype": body.get("NERtype"),
            "language": body.get("language"),
        }

        update = {
            "$set": {"candidates": final_result, "lastAccessed": datetime.datetime.now(datetime.timezone.utc)},
            "$setOnInsert": query,
        }

        try:
            self.candidate_cache_collection.update_one(query, update, upsert=True)
        except Exception as e:
            print(f"Error inserting or updating in cache: {e}")

    def _check_ids(self, name, kg, ids, ntoken_mention, length_mention, ambiguity_mention, corrects_tokens, result):
        if ids is None:
            return None

        result = result or []
        ids_list = ids.split(" ")
        for item in result:
            if item["id"] in ids_list:
                return None

        query = self.create_ids_query(ids)
        result_by_id = self.elastic_retriever.search(query, kg)
        result_by_id = self._get_final_candidates_list(
            result_by_id, name, kg, ambiguity_mention, corrects_tokens, ntoken_mention, length_mention
        )
        new_result = result + result_by_id
        return new_result

    def _get_types_id_to_name(self, ids, kg):
        items_collection = self.database.get_requested_collection("items", kg=kg)
        results = items_collection.find({"kind": "type", "entity": {"$in": ids}})
        types_id_to_name = {result["entity"]: result["labels"].get("en") for result in results}
        return types_id_to_name

    def create_token_query(self, name):
        query = {"query": {"match": {"name": name}}, "_source": {"excludes": ["language"]}}
        return query

    # Create a query to search for a list of ids (string separated by space)
    def create_ids_query(self, ids):
        # Base query
        query = {
            "query": {
                "bool": {
                    "must": [{"match": {"id": ids}}, {"match": {"language": "en"}}, {"match": {"is_alias": False}}]
                }
            },
            "_source": {
                "excludes": ["language"]
            }
        }
        return query

    def create_query(self, name, fuzzy=False, types=None, kind=None, NERtype=None, language=None):
        # Base query
        query_base = {
            "query": {"bool": {"must": [], "filter": []}}, "sort": [{"popularity": {"order": "desc"}}],
            "_source": {"excludes": ["language"]}
        }

        # Add name to the query
        if fuzzy:
            query_base["query"]["bool"]["must"].append({"match": {"name": {"query": name, "fuzziness": "auto"}}})
        else:
            query_base["query"]["bool"]["must"].append({"match": {"name": {"query": name, "boost": 2}}})

        # Add types filter if provided
        if types:
            query_base["query"]["bool"]["must"].append({"match": {"types": types}})

        # Add kind filter if provided
        if kind:
            query_base["query"]["bool"]["filter"].append({"term": {"kind": kind}})

        # Add NERtype filter if provided
        if NERtype:
            query_base["query"]["bool"]["filter"].append({"term": {"NERtype": NERtype}})

        # Add language filter if provided
        if language:
            query_base["query"]["bool"]["filter"].append({"term": {"language": language}})

        return query_base
