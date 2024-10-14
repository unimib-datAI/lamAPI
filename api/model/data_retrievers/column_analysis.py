import pandas as pd
from column_classifier.column_classifier import ColumnClassifier

class ColumnAnalysis:
    def __init__(self):
        pass

    def classify_columns(self, input_tables, model_type="accurate"):
        """
        Classify a list of columns with the specified model_type.
        
        Parameters:
        - columns (list): List of columns data to be classified.
        - model_type (str): The model type to use for classification ('accurate' or 'fast').
        
        Returns:
        - list: A list of classified columns in the adapted output format.
        """
        # Create a DataFrame from the list of columns
        df_list = []

        for columns in input_tables:
            df = pd.DataFrame(columns).transpose()
            df_list.append(df)
        
        # Initialize the ColumnClassifier
        classifier = ColumnClassifier(model_type=model_type)
        
        # Classify the DataFrame columns
        classification_results = classifier.classify_multiple_tables(df_list)
        
        # Process and return the adapted output format
        return self.generate_output_format(classification_results)
    
    def generate_output_format(self, classification_results):
        """
        Transform the classification results into the desired output format.
        
        Parameters:
        - classification_results (list): List of dictionaries containing the classification results.
        
        Returns:
        - list: A list of adapted output dictionaries with index_column, tag, classification, datatype, and probabilities.
        """
        adapted_output = []

        # Iterate through each table in the classification results
        for table_result in classification_results:
            for table_name, columns_info in table_result.items():
                table_output = {}
                
                # Iterate through each column's classification result
                for col_name, col_info in columns_info.items():
                    classification = col_info['classification']
                    probabilities = col_info['probabilities']

                    NE_types = ["PERSON", "ORGANIZATION", "LOCATION", "OTHER"]
                    lit_types = ["NUMBER", "DATE", "STRING"]

                    # Determine the tag, classification type, and datatype based on the classification
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

                    # Add the transformed data for the column to the table output
                    table_output[col_name] = {
                        "index_column": col_name,
                        "tag": tag,
                        "classification": classification_type,
                        "datatype": datatype,
                        "probabilities": probabilities
                    }

                # Append the table output to the final adapted output list
                adapted_output.append({table_name: table_output})

        return adapted_output