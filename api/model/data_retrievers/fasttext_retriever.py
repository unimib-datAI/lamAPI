import os
import requests


FASTTEXT_ENDPOINT = os.environ["FASTTEXT_ENDPOINT"]
FASTTEXT_TOKEN = os.environ["FASTTEXT_TOKEN"]


class FASTTEXTRetriever:

    def __init__(self):
        self.url = f"http://{FASTTEXT_ENDPOINT}/mention"

    def get_vectors(self, mentions):
        try:
            result = requests.post(f"{self.url}?token={FASTTEXT_TOKEN}", json=mentions)
            result = result.json()
        except Exception as e:
            result = {"status": "Error", "message": str(e)}
        
        return result        