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
        self.cache_collection_name = "bow"  # MongoDB collection for caching

        # Ensure indexes are created for efficient querying
        self.ensure_cache_indexes()

    def ensure_cache_indexes(self):
        """Ensure indexes on the cache collection."""
        cache_collection = self.database.get_requested_collection(self.cache_collection_name)
        cache_collection.create_index([("text", 1), ("id", 1)], unique=True, background=True)

    def get_bow_from_cache(self, text, entities, kg="wikidata"):
        """Retrieve cached results for the given text and entity IDs."""
        if not entities:
            return {}

        cache_collection = self.database.get_requested_collection(self.cache_collection_name, kg)
        query = {"text": text, "id": {"$in": entities}}
        results = list(cache_collection.find(query))

        # Convert results into a dictionary
        return {item["id"]: {"similarity_score": item["similarity_score"], "matched_words": item["matched_words"]} for item in results}

    def update_cache(self, text, results, kg="wikidata"):
        """Update the cache with new results."""
        if not results:
            return

        cache_collection = self.database.get_requested_collection(self.cache_collection_name, kg)
        for entity_id, result in results.items():
            cache_document = {
                "text": text,
                "id": entity_id,
                "similarity_score": result["similarity_score"],
                "matched_words": result["matched_words"],
            }
            cache_collection.update_one(
                {"text": text, "id": entity_id},  # Match by text and ID
                {"$set": cache_document},         # Update cache data
                upsert=True                        # Insert if not found
            )

    def get_bow(self, text, entities, kg="wikidata"):
        """Retrieve BoWs, using the cache first and falling back to the database."""
        if not entities:
            return {}

        # Step 1: Try to get results from the cache
        cached_results = self.get_bow_from_cache(text, entities, kg)

        # Step 2: Identify missing entities
        cached_entity_ids = set(cached_results.keys())
        missing_entities = [entity for entity in entities if entity not in cached_entity_ids]

        # Step 3: Retrieve missing BoWs from the database
        if missing_entities:
            items_retrieved = self.get_bow_from_db(missing_entities, kg)
            candidate_bows = {}
            for item in items_retrieved:
                entity_id = item["id"]
                bow_data = set(pickle.loads(gzip.decompress(item["bow"])).keys())
                candidate_bows[entity_id] = bow_data

            # Compute similarity for the missing entities
            computed_results = self.compute_bow_similarity(text, candidate_bows)

            # Update the cache with new results
            self.update_cache(text, computed_results, kg)

            # Merge cached and computed results
            cached_results.update(computed_results)

        return cached_results

    def get_bow_from_db(self, entities=None, kg="wikidata"):
        """Retrieve BoWs directly from the database."""
        if entities is None:
            entities = []
        if kg not in self.database.get_supported_kgs():
            raise ValueError(f"Knowledge graph '{kg}' is not supported.")
        
        query = {"id": {"$in": entities}}
        return self.database.get_requested_collection("items_vectors2", kg).find(query)

    def compute_bow_similarity(self, row_text, candidate_bows):
        """Compute similarity between the row BoW and candidate BoWs."""
        row_tokens = self.tokenize_text(row_text)
        row_token_count = len(row_tokens)

        result = {}
        for qid, candidate_bow_set in candidate_bows.items():
            intersection = row_tokens.intersection(candidate_bow_set)
            similarity = len(intersection) / row_token_count if row_token_count > 0 else 0
            result[qid] = {
                "similarity_score": round(similarity, 2),
                "matched_words": list(intersection),
            }

        return result

    def tokenize_text(self, text):
        """Tokenize and clean the text."""
        tokens = word_tokenize(text.lower())
        return set(t for t in tokens if t not in stop_words)
