import pandas as pd
from column_classifier.column_classifier import ColumnClassifier

class ColumnAnalysis:
    def __init__(self):
       pass

    def classify_columns(self, columns, model_type="fast"):
        df = pd.DataFrame(columns).transpose()
        # Initialize the classifier
        classifier = ColumnClassifier(model_type=model_type)

        # Classify the DataFrame columns
        classification_results = classifier.classify_dataframe(df)
        return self.generate_output_format(classification_results)
    

    def generate_output_format(self, input_data):
        adapted_output = {}

        # Iterate through each column in the input data
        for col_idx, col_info in input_data.items():
            index = int(col_idx)
            
            # Determine the classification and tag
            classification = col_info['classification']
            probabilities = col_info["probabilities"]

            if classification == "NUMBER":
                tag = "LIT"
                classification_type = "NUMBER"
                datatype = "NUMBER"  # For LIT, always set datatype as NUMBER
            elif classification == "DATE":
                tag = "LIT"
                classification_type = "DATE"
                datatype = "DATE"
            elif classification == "LOCATION":
                tag = "NE"
                classification_type = "ENTITY"
                # For NE, pick the datatype based on the winning probability
                datatype = max(probabilities, key=probabilities.get)
            else:
                tag = "LIT"
                classification_type = "STRING"  # Fallback for other cases
                datatype = "STRING"  # Default STRING for non-NE types
            
            # Add the transformed data to the adapted output, including probabilities
            adapted_output[col_idx] = {
                "index_column": index,
                "tag": tag,
                "classification": classification_type,
                "datatype": datatype,
                "probabilities": probabilities
            }

        return adapted_output