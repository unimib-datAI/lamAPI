import base64
# Download NLTK resources if not already downloaded
import nltk
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
import pickle
import gzip

# Global stopwords to avoid reinitializing repeatedly
stop_words = set(stopwords.words('english'))

class BOWRetriever:

    def __init__(self, database):
        self.database = database

    def get_bow_from_db(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"id": {"$in": entities}}
        return self.database.get_requested_collection("items_vectors2", kg).find(query)

    def get_bow(self, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        
        entity_bow = {}
        items_retrieved = self.get_bow_from_db(entities=entities, kg=kg)
        
        for item in items_retrieved:
            entity_id = item["id"]
            bow = item.get("bow", [])
            # Decode BoW (already base64 encoded) from database for each candidate
            compressed_bytes = base64.b64decode(bow)
            entity_bow[entity_id] = pickle.loads(gzip.decompress(compressed_bytes))
        
        return entity_bow

    def compute_common_words(self, row_bow_set, candidate_bows):
        """
        Computes the common words between row_bow and each candidate bow.
        row_bow: list of words representing the BoW for the row.
        candidate_bows: dictionary with keys as entity IDs and values as lists of words for each candidate BoW.
        """
        common_words_result = {}

        for entity_id, candidate_bow in candidate_bows.items():
            candidate_bow_set = set(candidate_bow)
            common_words = row_bow_set.intersection(candidate_bow_set)
            common_words_result[entity_id] = list(common_words)
        
        return common_words_result

    def get_bow_output(self, row_text, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")

        # Preprocess and create row BoW using nltk word_tokenize
        row_bow_set = self.tokenize_text(row_text)  # Tokenize with nltk
        
        # Retrieve candidate BoWs and compute common words
        candidate_bows = self.get_bow(entities, kg=kg)
        return self.compute_common_words(row_bow_set, candidate_bows)
    
    def tokenize_text(self, text):
        """Tokenize and clean the text."""
        tokens = word_tokenize(text.lower())
        return set(t for t in tokens if t not in stop_words)