class LabelsRetriever:

    def __init__(self, database):
        self.database = database

    def get_labels(self, entities=[], kg="wikidata", category=None):
        if kg in self.database.get_supported_kgs():
            if category is None:
                return self.database.get_requested_collection("items", kg).find({"entity": {"$in": list(entities)}})
            else:
                return self.database.get_requested_collection("items", kg).find(
                    {"entity": {"$in": list(entities)}, "category": category}
                )

    def get_labels_output(self, entities=[], kg="wikidata", lang=None, category=None):
        final_result = {}

        if kg in self.database.get_supported_kgs():
            final_result_wikidata = {}
            retrieved_wikidata_data = self.get_labels(entities, kg, category)
            for obj in retrieved_wikidata_data:
                final_result_wikidata[obj["entity"]] = {
                    "category": obj["category"],
                    "url": self.database.get_url_kgs()[kg] + obj["entity"],
                    "description": obj["description"].get("value"),
                }
                if lang in obj["labels"]:
                    final_result_wikidata[obj["entity"]]["labels"] = {}
                    final_result_wikidata[obj["entity"]]["labels"][lang] = obj["labels"][lang]
                else:
                    final_result_wikidata[obj["entity"]]["labels"] = obj["labels"]

                if lang in obj["aliases"]:
                    final_result_wikidata[obj["entity"]]["aliases"] = {}
                    final_result_wikidata[obj["entity"]]["aliases"][lang] = obj["aliases"][lang]
                else:
                    final_result_wikidata[obj["entity"]]["aliases"] = obj["aliases"]

            final_result[kg] = final_result_wikidata

        return final_result
