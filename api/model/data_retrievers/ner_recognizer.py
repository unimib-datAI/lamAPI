import spacy

class NERRecognizer:

    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")

    def recognize_entities(self, text_list):
        final_response = {}

        for index, text in enumerate(text_list):
            doc = self.nlp(text)

            ner = []
            for ent in doc.ents:
                print(ent.text, ent.start_char, ent.end_char, ent.label_)
                ner.append({'mention': ent.text, 'classification': ent.label_, 'start_index': ent.start_char, 'end_index': ent.end_char})
            
            final_response[f"{index}"] = {"text": text, "ner": ner}

        return final_response