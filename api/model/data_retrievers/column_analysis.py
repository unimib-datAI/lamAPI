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
        # Real-world entity types
        self.REAL_WORLD_ENTITY_TYPES = {"PERSON", "NORP", "FAC", "ORG", "GPE", "LOC", "PRODUCT", "EVENT", "WORK_OF_ART", "LAW", "LANGUAGE"}

    def sub_sample_column(self, column, sample_size=50):
        unique_values = list(set(column))
        if len(unique_values) <= sample_size:
            return unique_values
        return random.sample(unique_values, sample_size)

    def analyze_column_text(self, joined_text):
        """Analyze the joined text and return the frequency of each entity type."""
        doc = self.nlp(joined_text)
        entity_counts = defaultdict(int)

        for ent in doc.ents:
            if ent.label_ in self.REAL_WORLD_ENTITY_TYPES:
                entity_counts["ENTITY"] += 1
            elif ent.label_ in self.LIT_DATATYPE:
                entity_counts[ent.label_] += 1
            else:
                entity_counts["STRING"] += 1

        return entity_counts

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

            # Join the entire column into a single string separated by commas
            joined_column = ",".join(map(str, column))

            # Analyze the joined text
            entity_counts = self.analyze_column_text(joined_column)

            # Update labels and tags based on entity counts
            for label, count in entity_counts.items():
                update_dict(labels, label, count)
                tag = "NE" if label == "ENTITY" else "LIT"
                update_dict(tags, tag, count)

            # Determine the most frequent tag and label for the column
            winning_tag = "LIT" if tags["LIT"] >= tags["NE"] else "NE"
            winning_type, winning_datatype = self._get_winning_data_and_datatype(tags, labels, rows)

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