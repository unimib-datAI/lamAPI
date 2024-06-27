import random
from collections import defaultdict
import dateutil.parser


class ColumnAnalysis:
    def __init__(self, lookup_retriever):
        self.lookup_retriever = lookup_retriever
        self.LIT_DATATYPE = {
            "DATE": "DATETIME",
            "TIME": "STRING",
            "PERCENT": "STRING",
            "MONEY": "STRING",
            "QUANTITY": "STRING",
            "ORDINAL": "NUMBER",
            "CARDINAL": "NUMBER",
            "URL": "STRING",
            "EMAIL": "STRING",
            "INTEGER": "NUMBER",
            "FLOAT": "NUMBER",
            "DATETIME": "DATETIME",
            "STRING": "STRING",
            "DESC": "STRING",
        }

    def sub_sample_column(self, column, sample_size=50):
        unique_values = list(set(column))
        if len(unique_values) <= sample_size:
            return unique_values
        return random.sample(unique_values, sample_size)

    def classify_columns(self, columns=[]):
        def update_dict(dictionary, key, value=1):
            if key not in dictionary:
                dictionary[key] = 0
            dictionary[key] += value

        final_result = {}
        rows = len(columns[0])

        for index, column in enumerate(columns):
            column = self.sub_sample_column(column)
            final_result[index] = {}

            labels = defaultdict(int)
            tags = {"NE": 0, "LIT": 0}
            is_no_ann = False

            for cell in column:
                label = None

                # Check if the cell is a number
                try:
                    float(cell)
                    label = "CARDINAL"
                except:
                    pass

                # Check if the cell is a date if it's not already identified as a number
                if not label:
                    try:
                        dateutil.parser.parse(cell, fuzzy=False)
                        label = "DATE"
                    except:
                        pass

                # Check for other types
                if not label:
                    if len(cell.split(" ")) >= 20:
                        label = "DESC"
                    elif len(cell.split(" ")) == 1 and len(cell) <= 4:
                        label = "TOKEN"

                if label == "DESC":
                    is_no_ann = True

                if label is not None:
                    update_dict(labels, label)
                    tag = "LIT" if label in self.LIT_DATATYPE else "NE"
                    update_dict(tags, tag)
                else:
                    label = self.check_literal(cell)
                    if label != "STRING":
                        update_dict(labels, label)
                        tag = "LIT" if label in self.LIT_DATATYPE else "NE"
                        update_dict(tags, tag)

            if is_no_ann:
                final_result[index] = {
                    "index_column": index,
                    "tag": "LIT",
                    "classification": "DESC",
                    "datatype": "NO_ANN",
                    "column_rows": column,
                }
                continue

            # Determine preliminary classification
            winning_tag, winning_type, winning_datatype = self._get_winning_data_and_datatype(tags, labels, rows)

            # If preliminary classification is STRING and not confidently identified (pure STRING), use lookup to refine
            if (
                winning_datatype == "STRING"
                and not (labels.get("URL") or labels.get("EMAIL"))
                or winning_datatype == None
            ):
                combined_scores = []
                entity_types = defaultdict(int)
                type_details = defaultdict(list)
                for cell in column:
                    lookup_result = self.lookup_retriever.search(cell)
                    cell = list(lookup_result.keys())[0]
                    lookup_result = lookup_result[cell]
                    if lookup_result:
                        sorted_lookup_result = sorted(
                            lookup_result, key=lambda x: (x["ed_score"] + x["jaccard_score"]) / 2, reverse=True
                        )[
                            :3
                        ]  # Top 3 results after sorting
                        for entity in sorted_lookup_result:
                            combined_scores.append((entity["ed_score"] + entity["jaccard_score"]) / 2)
                            for entity_type in entity["types"]:
                                update_dict(entity_types, entity_type["name"])
                                type_details[entity_type["name"]].append(
                                    {"id": entity_type["id"], "name": entity_type["name"]}
                                )

                if combined_scores and sum(combined_scores) / len(combined_scores) >= 0.65:
                    winning_tag = "NE"
                    winning_type = "ENTITY"
                    winning_datatype = "ENTITY"
                    # Get the top 3 most frequent entity types with their scores
                    most_frequent_types = sorted(entity_types.items(), key=lambda x: x[1], reverse=True)
                    probable_types = [
                        {"id": type_details[t[0]][0]["id"], "name": t[0], "frequency": t[1]}
                        for t in most_frequent_types
                    ]
                else:
                    winning_tag = "LIT"
                    winning_type = "STRING"
                    winning_datatype = "STRING"
                    probable_types = None

            final_result[index] = {
                "index_column": index,
                "tag": winning_tag,
                "classification": winning_type,
                "datatype": winning_datatype,
                "column_rows": column,
            }

            if winning_tag == "NE":
                final_result[index]["type_estimation"] = probable_types

        return final_result

    def check_literal(self, cell):
        # Implement literal check logic here
        if "@" in cell and "." in cell:
            return "EMAIL"
        elif cell.startswith("http://") or cell.startswith("https://"):
            return "URL"
        elif any(char.isdigit() for char in cell):
            try:
                float(cell)
                return "FLOAT" if "." in cell else "INTEGER"
            except:
                pass
        return "STRING"

    def _get_winning_data_and_datatype(self, tags, labels, rows):
        winning_tag = "LIT" if tags["LIT"] >= tags["NE"] else "NE"
        winning_type = None
        winning_datatype = "STRING"

        if winning_tag == "LIT":
            new_labels = {label: labels.get(label, 0) for label in self.LIT_DATATYPE}
            label_max = max(new_labels, key=new_labels.get, default=None)
            if labels.get(label_max, 0) >= rows * 0.50:
                winning_type = label_max
            else:
                winning_type = "STRING"

        winning_datatype = self.LIT_DATATYPE.get(winning_type)

        return winning_tag, winning_type, winning_datatype
