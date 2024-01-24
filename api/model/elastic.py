import os
import subprocess
import json
from elasticsearch import Elasticsearch

ELASTIC_USER = os.environ["ELASTICSEARCH_USERNAME"]
ELASTIC_PW = os.environ["ELASTIC_PASSWORD"]
ELASTIC_LIMITS = 100
ELASTIC_ENDPOINT, ELASTIC_PORT = os.environ["ELASTIC_ENDPOINT"].split(":")
MAX_POPULARITY = 873

with open("index_mappings.json") as f:
    indexes_mappings = json.loads(f.read())


if "ELASTIC_FINGERPRINT" not in os.environ or len(os.environ["ELASTIC_FINGERPRINT"]) == 0:
    bashCommand = """
                    openssl s_client -connect es01:9200 -servername es01 -showcerts </dev/null 2>/dev/null | 
                    openssl x509 -fingerprint -sha256 -noout -in /dev/stdin
                """
    p = subprocess.Popen(
        bashCommand, 
        stdout=subprocess.PIPE, shell=True)
    output = p.communicate()
    fingerprint = output[0].decode("UTF-8")
    CERT_FINGERPRINT = fingerprint.split("=")[1][0:-1]
else:
    CERT_FINGERPRINT = os.environ["ELASTIC_FINGERPRINT"]

class Elastic:
    def __init__(self, timeout=120):
        self._elastic = Elasticsearch(
            hosts=f'https://{ELASTIC_ENDPOINT}:{ELASTIC_PORT}',
            request_timeout=60,
            max_retries=10, 
            retry_on_timeout=True,
            basic_auth=(ELASTIC_USER, ELASTIC_PW),
            ssl_assert_fingerprint=CERT_FINGERPRINT
        )
        self._timeout = timeout
       
    def get_index(self, kg):
        indexes_to_filter_out = set(indexes_mappings[kg]["indexes_to_filter_out"])
        indexes = indexes_mappings[kg]["indexes"]
        indexes_to_consider = [indexes[category] for category in indexes if category not in indexes_to_filter_out]
        return indexes_to_consider
    
    def search(self, body, kg = "wikidata", size=100):
        self._index_name = self.get_index(kg)
        
        query_result = self._elastic.search(index=self._index_name, query=body["query"], size=size)

        hits = query_result["hits"]["hits"]
        max_score = query_result["hits"]["max_score"]
        if len(hits) == 0:
            return [], {}

        new_hits = []
        index_sources = {}
        if kg in indexes_mappings:

            for i, hit in enumerate(hits):
                new_hits.append({
                    "id": hit["_source"]["id"],
                    "name": hit["_source"]["name"],
                    "description": hit["_source"]["description"],
                    "types": hit["_source"]["types"],
                    "popularity": round(hit["_source"]["popularity"]/MAX_POPULARITY, 2),
                    "pos_score": round((i+1)/len(hits), 3),
                    "es_score": round(hit["_score"]/max_score, 3),
                    "ntoken_entity": hit["_source"]["ntoken"],
                    "length_entity": hit["_source"]["length"]
                })
                index_sources[hit["_source"]["id"]] = hit["_index"]
             
        
        return new_hits, index_sources
        