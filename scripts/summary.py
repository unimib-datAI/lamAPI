import os
import statistics
import sys
import time

from pymongo import MongoClient


def get_mongo_client():
    try:
        mongo_endpoint, mongo_endpoint_port = os.environ["MONGO_ENDPOINT"].split(":")
        mongo_endpoint_username = os.environ["MONGO_INITDB_ROOT_USERNAME"]
        mongo_endpoint_password = os.environ["MONGO_INITDB_ROOT_PASSWORD"]
    except KeyError as e:
        sys.exit(f"Environment variable {str(e)} not set.")

    return MongoClient(
        mongo_endpoint, int(mongo_endpoint_port), username=mongo_endpoint_username, password=mongo_endpoint_password
    )


client = get_mongo_client()


def fetch_predicate_labels(predicate_ids, collection):
    predicates = collection.find({"entity": {"$in": predicate_ids}}, {"entity": 1, "labels.en": 1})
    predicate_labels = {
        predicate["entity"]: predicate.get("labels", {}).get("en", "Unknown Label") for predicate in predicates
    }
    return predicate_labels


def enhance_and_store_results(db_name, collection_name, summary_collection_name, pipeline, label_resolver_collection):
    db = client[db_name]
    collection = db[collection_name]
    results = collection.aggregate(pipeline)
    aggregated_results = list(results)

    batch_size = 1000
    buffer = []
    id_predicates = []
    distribution = [result["count"] for result in aggregated_results]
    distribution_mean = statistics.mean(distribution)
    distribution_stdev = statistics.stdev(distribution)
    distribution_max = max(distribution)
    distribution_min = min(distribution)
    distribution_sum = sum(distribution)

    for result in aggregated_results:
        if collection_name == "objects":
            id_predicates.append(result["_id"])
            buffer.append(
                {
                    "predicate": result["_id"],
                    "label": None,
                    "count": result["count"],
                    "countNormSumAll": round(result["count"] / distribution_sum, 2),
                    "countNormMax": round(result["count"] / distribution_max, 2),
                    "countNormMinMax": round(
                        (result["count"] - distribution_min) / (distribution_max - distribution_min), 2
                    ),
                    "countNormZScore": round((result["count"] - distribution_mean) / distribution_stdev, 2),
                }
            )
        else:
            id_predicates.append(result["_id"]["predicate"])
            buffer.append(
                {
                    "predicate": result["_id"]["predicate"],
                    "label": None,
                    "count": result["count"],
                    "countNormSumAll": round(result["count"] / distribution_sum, 2),
                    "countNormMax": round(result["count"] / distribution_max, 2),
                    "countNormMinMax": round(
                        (result["count"] - distribution_min) / (distribution_max - distribution_min), 2
                    ),
                    "countNormZScore": round((result["count"] - distribution_mean) / distribution_stdev, 2),
                }
            )

        if len(buffer) == batch_size:
            predicate_labels = fetch_predicate_labels(id_predicates, db[label_resolver_collection])
            for i, item in enumerate(buffer):
                item["label"] = predicate_labels.get(id_predicates[i], "Unknown Label")
            summary_collection = db[summary_collection_name]
            summary_collection.insert_many(buffer)
            buffer = []
            id_predicates = []

    if len(buffer) > 0:
        predicate_labels = fetch_predicate_labels(id_predicates, db[label_resolver_collection])
        for i, item in enumerate(buffer):
            item["label"] = predicate_labels.get(id_predicates[i], "Unknown Label")
        summary_collection = db[summary_collection_name]
        summary_collection.insert_many(buffer)
        buffer = []
        id_predicates = []

    summary_collection.create_index([("count", -1)])


def main(db_name):
    start_time_objects = time.time()
    pipeline_objects = [
        {"$project": {"relationPairs": {"$objectToArray": "$objects"}}},
        {"$unwind": "$relationPairs"},
        {"$unwind": "$relationPairs.v"},
        {"$group": {"_id": "$relationPairs.v", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
    ]
    enhance_and_store_results(db_name, "objects", "objectsSummary", pipeline_objects, "items")

    end_time_objects = time.time()
    print(f"Time taken for objects: {end_time_objects - start_time_objects} seconds")

    start_time_literals = time.time()
    pipeline_literals = [
        {"$project": {"literals": {"$objectToArray": "$literals"}}},
        {"$unwind": "$literals"},
        {"$unwind": "$literals.v"},
        {"$project": {"literalType": "$literals.k", "predicateValuePairs": {"$objectToArray": "$literals.v"}}},
        {"$unwind": "$predicateValuePairs"},
        {
            "$group": {
                "_id": {"literalType": "$literalType", "predicate": "$predicateValuePairs.k"},
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]
    enhance_and_store_results(db_name, "literals", "literalsSummary", pipeline_literals, "items")

    end_time_literals = time.time()
    print(f"Time taken for literals: {end_time_literals - start_time_literals} seconds")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        db_name = sys.argv[1]
        main(db_name)
    else:
        sys.exit("Please provide a DB name as an argument.")
