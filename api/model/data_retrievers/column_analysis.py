import random
from collections import defaultdict
import dateutil.parser
import spacy

class ColumnAnalysis:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
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

        def is_entity(cell):
            doc = self.nlp(cell)
            return any(ent.label_ for ent in doc.ents)

        final_result = {}
        rows = len(columns[0])

        for index, column in enumerate(columns):
            column = self.sub_sample_column(column)
            final_result[index] = {}

            labels = defaultdict(int)
            tags = {"NE": 0, "LIT": 0}
            is_no_ann = False
            comma_count = 0
            pattern_detected = False

            for cell in column:
                label = None

                # Normalize the cell by removing commas (for large numbers)
                normalized_cell = cell.replace(",", "")

                # Count commas to determine if the column might be a list or category
                comma_count += cell.count(',')

                # Check if the cell is a number
                try:
                    float(normalized_cell)
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

                # Check if the cell is a named entity (location, postcode, country, etc.)
                if not label:
                    doc = self.nlp(cell)
                    for ent in doc.ents:
                        if ent.label_ in {"GPE", "LOC", "ORG"}:  # Use Spacy entity types for places and organizations
                            label = "ENTITY"
                            break

                # Consistent pattern detection (e.g., "success: 200" or "error: 404")
                if not label and (":" in cell and len(cell.split(":")) == 2):
                    pattern_detected = True
                    label = "STATUS"

                # Check for other types
                if not label:
                    if len(cell.split(" ")) >= 20:
                        label = "DESC"
                    elif len(cell.split(" ")) == 1 and len(cell) <= 4:
                        label = "TOKEN"
                    else:
                        label = self.check_literal(cell)

                if label == "DESC":
                    is_no_ann = True

                if label is not None:
                    update_dict(labels, label)
                    tag = "LIT" if label in self.LIT_DATATYPE else "NE"
                    update_dict(tags, tag)
                else:
                    # If no label was identified, default to STRING
                    label = "STRING"
                    update_dict(labels, label)
                    update_dict(tags, "LIT")

            # Check if a significant number of rows contain commas, indicating a list-like structure
            if comma_count / len(column) > 0.5:
                final_result[index] = {
                    "index_column": index,
                    "tag": "LIT",
                    "classification": "STRING",
                    "datatype": "STRING",
                    "column_rows": column,
                }
                continue

            if pattern_detected:
                # If patterns like "success: 200" are detected, and they are dominant in the column, classify as LIT
                final_result[index] = {
                    "index_column": index,
                    "tag": "LIT",
                    "classification": "STRING",
                    "datatype": "STRING",
                    "column_rows": column,
                }
                continue

            if is_no_ann:
                final_result[index] = {
                    "index_column": index,
                    "tag": "LIT",
                    "classification": "DESC",
                    "datatype": "NO_ANN",
                    "column_rows": column,
                }
                continue

            # Ensure NE classification if geographical locations are detected
            if "ENTITY" in labels and labels["ENTITY"] >= rows * 0.50:
                winning_tag = "NE"
                winning_type = "ENTITY"
                winning_datatype = "STRING"  # or a custom type if needed
            else:
                # Determine preliminary classification
                winning_tag, winning_type, winning_datatype = self._get_winning_data_and_datatype(tags, labels, rows)

            final_result[index] = {
                "index_column": index,
                "tag": winning_tag,
                "classification": winning_type,
                "datatype": winning_datatype,
                "column_rows": column,
            }

        return final_result

    def check_literal(self, cell):
        # Enhanced literal check logic
        if "@" in cell and "." in cell:
            return "EMAIL"
        elif cell.startswith("http://") or cell.startswith("https://"):
            return "URL"
        elif any(char.isdigit() for char in cell):
            normalized_cell = cell.replace(",", "")
            try:
                float(normalized_cell)
                return "FLOAT" if "." in normalized_cell else "INTEGER"
            except:
                pass
        elif len(cell.split(" ")) > 1:
            return "ENTITY"
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