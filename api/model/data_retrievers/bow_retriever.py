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
        
        entity_bow = {entity:set() for entity in entities}
        items_retrieved = self.get_bow_from_db(entities=entities, kg=kg)
        
        for item in items_retrieved:
            entity_id = item["id"]
            bow = item.get("bow", [])
            entity_bow[entity_id] = set(pickle.loads(gzip.decompress(bow)).keys())  # Decompress the BoW
        
        return entity_bow

    def compute_bow_similarity(self, row_text, candidate_bows):
        """Computes both the row-length-dependent similarity and matched words for the row BoW and candidate BoWs."""
        row_tokens = self.tokenize_text(row_text)
        row_token_count = len(row_tokens)

        result = {}
        for qid, candidate_bow_set in candidate_bows.items():
            intersection = row_tokens.intersection(candidate_bow_set)
            
            # Row-length-dependent similarity calculation
            similarity = len(intersection) / row_token_count if row_token_count > 0 else 0  
            
            # Truncate similarity to 2 decimal places for clarity
            result[qid] = {
                "similarity_score": round(similarity, 2),
                "matched_words": list(intersection)
            }

        return result

    def get_bow_output(self, row_text, entities=None, kg="wikidata"):
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")

        # Preprocess and create row BoW using nltk word_tokenize
        row_bow_set = self.tokenize_text(row_text)  # Tokenize with nltk
        
        # Retrieve candidate BoWs
        candidate_bows = self.get_bow(entities, kg=kg)
        
        # Compute similarity scores and matched words with QID association
        result = self.compute_bow_similarity(row_text, candidate_bows)
        
        return result
    
    def tokenize_text(self, text):
        """Tokenize and clean the text."""
        tokens = word_tokenize(text.lower())
        return set(t for t in tokens if t not in stop_words)