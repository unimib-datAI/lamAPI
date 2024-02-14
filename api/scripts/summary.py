import sys
import os
from pymongo import MongoClient
import time

 # Initialize the MongoDB client with the URI
MONGO_ENDPOINT, MONGO_ENDPOINT_PORT = os.environ["MONGO_ENDPOINT"].split(":")
MONGO_ENDPOINT_USERNAME = os.environ["MONGO_INITDB_ROOT_USERNAME"]
MONGO_ENDPOINT_PASSWORD = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
client = MongoClient(MONGO_ENDPOINT, int(MONGO_ENDPOINT_PORT), username=MONGO_ENDPOINT_USERNAME, password=MONGO_ENDPOINT_PASSWORD)

def fetch_predicate_labels(predicate_ids, collection):
    predicates = collection.find({"entity": {"$in": predicate_ids}})
    
    # Mapping predicate IDs to labels
    predicate_labels = {}
    for predicate in predicates:
        # Assuming you want to use the English label ('en'), adjust as necessary
        label = predicate.get('labels', {}).get('en', '')
        if label:
            predicate_labels[predicate['entity']] = label
    
    return predicate_labels


def compute_and_store_summary(db_name, collection_name):
    # Access the database and collection
    db = client[db_name]
    collection = db[collection_name]

    pipeline = [
        { "$project": {
            "relationPairs": {
                "$objectToArray": "$objects"
            }
        }},
        { "$unwind": "$relationPairs" },
        { "$unwind": "$relationPairs.v" },
        {
            "$group": {
                "_id": "$relationPairs.v",
                "count": { "$sum": 1 }
            }
        },
        { "$sort": { "count": -1 } }
    ]

    cursor = collection.aggregate(pipeline)
    aggregated_results = []
    for doc in cursor:
        aggregated_results.append(doc)

    # Extract unique predicate IDs
    unique_predicates = list({result['_id'] for result in aggregated_results})
    # Fetch labels for the unique predicates
    predicate_labels = fetch_predicate_labels(unique_predicates, collection)

    # Example of enhancing aggregated results with labels
    enhanced_results = []
    for result in aggregated_results:
        predicate_id = result['_id']
        label = predicate_labels.get(predicate_id, "Unknown Label")  # Default to "Unknown Label" if not found
        enhanced_result = {
            'predicate': predicate_id,
            'label': label,
            'count': result['count']
        }
        enhanced_results.append(enhanced_result)

    # Store the enhanced results in a new collection
    db["summaryObjects"].insert_many(enhanced_results)


def compute_and_store_literals_summary(db_name, collection_name):
    # Access the database and collection
    db = client[db_name]
    collection = db[collection_name]

    pipeline = [
        { "$limit": 1000000 },
        {
            "$project": {
                "literals": {"$objectToArray": "$literals"}
            }
        },
        {"$unwind": "$literals"},
        {"$unwind": "$literals.v"},
        {"$project": {
            "literalType": "$literals.k",
            "predicateValuePairs": {"$objectToArray": "$literals.v"}
        }},
        {"$unwind": "$predicateValuePairs"},
        {"$group": {
            "_id": {
                "literalType": "$literalType",
                "predicate": "$predicateValuePairs.k"
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]

    results = collection.aggregate(pipeline)
    aggregated_results = []
    for result in results:
        aggregated_results.append(result)

    # Extract unique predicate IDs
    unique_predicates = list({result['_id']['predicate'] for result in aggregated_results})

    # Fetch labels for the unique predicates
    predicate_labels = fetch_predicate_labels(unique_predicates, collection)

    # Example of enhancing aggregated results with labels
    enhanced_results = []
    for result in aggregated_results:
        predicate_id = result['_id']['predicate']
        label = predicate_labels.get(predicate_id, "Unknown Label")  # Default to "Unknown Label" if not found
        enhanced_result = {
            'literalType': result['_id']['literalType'],
            'predicate': predicate_id,
            'label': label,
            'count': result['count']
        }
        enhanced_results.append(enhanced_result)

    # Store the enhanced results in a new collection
    db["summaryLiterals"].insert_many(enhanced_results)



if __name__ == "__main__":
    try:
        db_name = sys.argv[1:][0]
    except:
        sys.exit("Please provide a DB name as argument")

    start_time_objects = time.time()
    compute_and_store_summary(db_name, "objects")
    end_time_objects = time.time()
    objects_duration = end_time_objects - start_time_objects
    print(f"Time taken for objects: {objects_duration} seconds")

    start_time_literals = time.time()
    compute_and_store_literals_summary(db_name, "literals")
    end_time_literals = time.time()
    literals_duration = end_time_literals - start_time_literals
    print(f"Time taken for literals: {literals_duration} seconds")
