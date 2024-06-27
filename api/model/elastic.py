import os
from elasticsearch import Elasticsearch, ConnectionError
from time import sleep

# Extract environment variables
ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")


class Elastic:
    def __init__(self, timeout=120):
        self._timeout = timeout
        self._elastic = self.connect_to_elasticsearch()

    def connect_to_elasticsearch(self, max_retry=5, delay=10):
        retry = 0
        while retry < max_retry:
            try:
                es = Elasticsearch(
                    hosts=f'http://{ELASTIC_ENDPOINT}:{ELASTIC_PORT}',
                    request_timeout=60
                )
                if es.ping():
                    print("Connected to Elasticsearch")
                    return es
                else:
                    print("Unable to ping Elasticsearch")
            except ConnectionError as e:
                print(f"Connection error: {e}", flush=True)
            print(f"Retrying in {delay} seconds...")
            retry += 1
            sleep(delay)
        raise Exception("Failed to connect to Elasticsearch after multiple attempts")

    
    def search(self, body, kg="wikidata", limit=100):
        try:
            query_result = self._elastic.search(index=kg, query=body["query"], size=limit)
            hits = query_result["hits"]["hits"]
            max_score = query_result["hits"]["max_score"]
            if len(hits) == 0:
                return [], {}

            new_hits = []
           
            for i, hit in enumerate(hits):
                new_hit = {
                    "id": hit["_source"]["id"],
                    "name": hit["_source"]["name"],
                    "description": hit["_source"]["description"],
                    "types": hit["_source"]["types"],
                    "popularity": hit["_source"]["popularity"],
                    "pos_score": round((i + 1) / len(hits), 3),
                    "es_score": round(hit["_score"] / max_score, 3),
                    "ntoken_entity": hit["_source"]["ntoken"],
                    "length_entity": hit["_source"]["length"]
                }
                if "kind" in hit["_source"]:
                    new_hit["kind"] = hit["_source"]["kind"]
                    new_hit["NERtype"] = hit["_source"]["NERtype"]
                new_hits.append(new_hit)
            return new_hits
        except ConnectionError as e:
            print(f"Search connection error: {e}", flush=True)
            return [], {}
