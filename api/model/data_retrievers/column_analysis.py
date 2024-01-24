from model.literal_recognizer import LiteralRecognizer
import spacy
import re

nlp = spacy.load("en_core_web_sm")

class ColumnAnalysis:

    def __init__(self):
        self.literal_recognizer = LiteralRecognizer()
       
        self.entity_type_dict = {
            "PERSON": "NE",
            "NORP": "NE",
            "FAC": "NE",
            "ORG": "NE",
            "GPE": "NE",
            "LOC": "NE",
            "PRODUCT": "NE",
            "EVENT": "NE",
            "WORK_OF_ART": "NE",
            "LAW": "NE",
            "LANGUAGE": "NE",
            "DATE": "LIT",
            "TIME": "LIT",
            "PERCENT": "LIT",
            "MONEY": "LIT",
            "QUANTITY": "LIT",
            "ORDINAL": "LIT",
            "CARDINAL": "LIT",
            "URL": "LIT",
            "DESC": "LIT",
            "TOKEN": "NE",
            "INTEGER": "LIT",
            "FLOAT": "LIT",
            "DATETIME": "LIT",
            "EMAIL": "LIT"
        }

        self.LIT_DATATYPE = {
            "DATE": "DATETIME", 
            "TIME": "STRING", 
            "PERCENT": "STRING", 
            "MONEY": "STRING", 
            "QUANTITY": "STRING", 
            "ORDINAL": "NUMBER", 
            "CARDINAL": "NUMBER", 
            "URL": "STRING",
            "DESC": "STRING",
            "TOKEN": "STRING",
            "INTEGER": "NUMBER",
            "FLOAT": "NUMBER",
            "DATETIME": "DATETIME",
            "EMAIL": "STRING",
            "STRING": "STRING"
        }

        self.NE_DATATYPE = ["PERSON", "NORP", "FAC", "ORG", "GPE", "LOC", "PRODUCT", "EVENT", "WORK_OF_ART", "LAW", "LANGUAGE"]


    def classifiy_columns(self, columns = []):
       
        def update_dict(dictionary, key, value=1):
            if key not in dictionary:
                dictionary[key] = 0
            dictionary[key] += value    

        final_result = {}
        rows = len(columns[0])
        for index, column in enumerate(columns):
            final_result[index] = {} 
            
            # Analyze the concatenated text using Spacy
            labels = {}
            tags = {"NE": 0, "LIT": 0}

            for cell in column:
                is_number = False
                label = None
                try:
                    float(cell)
                    is_number = True
                except:
                    pass
            
                if is_number:
                    label = "CARDINAL"
                elif len(cell.split(" ")) >= 7:
                    label = "DESC"
                elif len(cell.split(" ")) == 1 and len(cell) <= 4:
                    label = "TOKEN"
                
                if label is not None:
                    update_dict(labels, label)
                    tag = self.entity_type_dict[label]
                    update_dict(tags, tag)
                    
                label = self.literal_recognizer.check_literal(cell)  
                
                if label != "STRING":
                    update_dict(labels, label)
                    tag = self.entity_type_dict[label]
                    update_dict(tags, tag)
        
  
            text_to_analyze = " ; ".join(column)
            doc = nlp(text_to_analyze)
            for ent in doc.ents:
                label = ent.label_
                if label in ["CARDINAL", "ORDINAL"]:
                    continue
                update_dict(labels, label)
                tag = self.entity_type_dict[label]
                update_dict(tags, tag)

            
            winning_tag, winning_type, winning_datatype = self._get_winning_data_and_datatype(tags, labels, rows)

            final_result[index] = {
                'index_column': index,
                'tag': winning_tag,
                'classification': winning_type,
                'datatype': winning_datatype,
                'column_rows': column
            }
        return final_result
    

    def _get_winning_data_and_datatype(self, tags, labels, rows):
        winning_tag = "NE"
        winning_type = None
        winning_datatype = None
        if tags["LIT"] + tags["NE"] == 0:
            winning_tag = "LIT"
            winning_datatype = "STRING"
        elif tags["NE"] > rows * 2:
            winning_tag = "LIT"
            winning_datatype = "STRING"
        elif tags["LIT"] >= tags["NE"]:
            winning_tag = "LIT"
        elif tags["NE"] <= rows * 0.40:
            winning_tag = "LIT"    
    
        
        if labels.get("DATE") == labels.get("CARDINAL"):
            if "CARDINAL" in labels:
                labels["CARDINAL"] += 1
        
        if winning_tag == "LIT":
            new_labels = {label:labels.get(label, 0) for label in self.LIT_DATATYPE}
            label_max = max(new_labels, key=new_labels.get, default=None)
            if labels.get(label_max, 0) >= rows * 0.50 and winning_datatype is None:
                winning_type = label_max    
            else:
                winning_type = "STRING"
        else:
            new_labels = {label:labels.get(label, 0) for label in self.NE_DATATYPE}
            label_max = max(new_labels, key=new_labels.get, default=None)
            winning_type = label_max  

        winning_datatype = self.LIT_DATATYPE.get(winning_type)
       
        return winning_tag, winning_type, winning_datatype
