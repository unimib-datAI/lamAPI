import pandas as pd
from column_classifier.column_classifier import ColumnClassifier

class ColumnAnalysis:
    def __init__(self):
       pass

    def classify_columns(self, columns, model_type="accurate"):
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
            
            NE_types = ["PERSON", "ORGANIZATION", "LOCATION", "OTHER"]
            lit_types = ["NUMBER", "DATE", "STRING"]

            if classification in lit_types:
                tag = "LIT"
                classification_type = classification
                datatype = classification
            elif classification in NE_types:
                tag = "NE"
                classification_type = classification
                datatype = classification
            else:
                tag = "UNKNOWN"
                classification_type = "UNKNOWN"
                datatype = "UNKNOWN"    
           
            
            # Add the transformed data to the adapted output, including probabilities
            adapted_output[col_idx] = {
                "index_column": index,
                "tag": tag,
                "classification": classification_type,
                "datatype": datatype,
                "probabilities": probabilities
            }

        return adapted_output