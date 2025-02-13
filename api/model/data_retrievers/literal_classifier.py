from model.literal_recognizer import LiteralRecognizer


class LiteralClassifier:

    def __init__(self):
        self.literal_recognizer = LiteralRecognizer()
        self.xml_datatypes = {
            "DATE": {"datatype": "DATE", "classification": "DATETIME", "tag": "LIT", "xml_datatype": "xsd:date"},
            "DATETIME": {
                "datatype": "DATETIME",
                "classification": "DATETIME",
                "tag": "LIT",
                "xml_datatype": "xsd:dateTime",
            },
            "TIME": {"datatype": "TIME", "classification": "DATETIME", "tag": "LIT", "xml_datatype": "xsd:time"},
            "URL": {"datatype": "URL", "classification": "STRING", "tag": "LIT", "xml_datatype": "xs:anyURI"},
            "EMAIL": {"datatype": "EMAIL", "classification": "STRING", "tag": "LIT", "xml_datatype": "xsd:string"},
            "INTEGER": {"datatype": "INTEGER", "classification": "NUMBER", "tag": "LIT", "xml_datatype": "xsd:integer"},
            "FLOAT": {"datatype": "FLOAT", "classification": "NUMBER", "tag": "LIT", "xml_datatype": "xsd:decimal"},
            "STRING": {"datatype": "STRING", "classification": "STRING", "tag": "NE", "xml_datatype": "xsd:string"},
        }

    def classifiy_literal(self, literals_list):
        final_response = {}
        for literal in literals_list:
            classification = self.literal_recognizer.check_literal(literal)
            final_response[literal] = self.xml_datatypes[classification]

        return final_response
