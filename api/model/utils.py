import re

import nltk
from model.database import Database


def editdistance(s1, s2):
    return 1 - nltk.edit_distance(s1, s2) / max(len(s1), len(s2))


# entity recognizer
def recognize_entity(entity):
    wikidata_pattern_obj = r"^Q\d+$"
    wikidata_pattern_pred = r"^P\d+$"
    if re.compile(wikidata_pattern_obj).search(entity) or re.compile(wikidata_pattern_pred).search(entity):
        return "wikidata"
    else:
        return "dbpedia"


# return splitted entities in correct knowledge graph
def split_different_kg_entities(entities=[]):
    final_splitting = {"wikidata": [], "dbpedia": []}
    for entity in entities:
        final_splitting[recognize_entity(entity)].append(entity)

    return final_splitting


def get_kgs(kg_specified):
    if kg_specified == "wikidata":
        return Database.WIKIDATA
    elif kg_specified == "dbpedia":
        return Database.DBPEDIA
    elif kg_specified == "crunchbase":
        return Database.CRUNCHBASE


def build_error(message, error_code, traceback=None):
    return {"error": message, "stacktrace": traceback}, error_code


def clean_str(s):
    s = s.lower()
    return " ".join(s.split())


def compute_similarity_between_string(str1, str2, ngram=None):
    ngrams_str1 = get_ngrams(str1, ngram)
    ngrams_str2 = get_ngrams(str2, ngram)
    score = len(ngrams_str1.intersection(ngrams_str2)) / max(len(ngrams_str1), len(ngrams_str2), 1)
    return score


def word2ngrams(text, n=None):
    """Convert word into character ngrams."""
    if n is None:
        n = len(text)
    return [text[i : i + n] for i in range(len(text) - n + 1)]


def get_ngrams(text, n=3):
    ngrams = set()
    for token in text.split(" "):
        temp = word2ngrams(token, n)
        for ngram in temp:
            ngrams.add(ngram)
    return set(ngrams)


def create_index(db):
    for kg in db.mappings:
        candidate_cache_collection = db.get_requested_collection("candidate", kg=kg)
        candidate_cache_collection.create_index(
            [("cell", 1), ("fuzzy", 1), ("ngrams", 1), ("type", 1), ("description", 1), ("kg", 1), ("limit", 1)],
            unique=True,
        )
