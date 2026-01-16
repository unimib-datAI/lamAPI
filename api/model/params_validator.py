import os
from model.utils import build_error

ACCESS_TOKEN = os.environ["LAMAPI_TOKEN"]
SENSITIVE_KG_TOKEN = os.environ.get("LAMAPI_SENSITIVE_KG_TOKEN")
SENSITIVE_KGS = [kg.strip().lower() for kg in os.environ.get("LAMAPI_SENSITIVE_KGS", "").split(",") if kg.strip()]

class ParamsValidator:
    def validate_token(self, token, kg=None):
        normalized_kg = kg.lower() if isinstance(kg, str) else None
        if normalized_kg and normalized_kg in SENSITIVE_KGS:
            if not SENSITIVE_KG_TOKEN:
                return False, build_error("Sensitive KG access token is not configured", 403)
            if token != SENSITIVE_KG_TOKEN:
                return False, build_error("Invalid access token", 403)
            return True, None
        if token != ACCESS_TOKEN:
            return False, build_error("Invalid access token", 403)
        else:
            return True, None

    def validate_kg(self, database, kg):
        print("kg", kg, flush=True)
        if kg is None:
            return True, "wikidata"
        elif kg not in database.get_supported_kgs():
            return False, build_error("Knowledge Graph Specification Error", 400)
        else:
            return True, kg

    def validate_limit(self, limit):
        if limit is None:
            return True, 1000
        try:
            limit = int(limit)
            return True, limit
        except Exception:

            return False, build_error("limit parameter cannot be converted to int", 400)

    def validate_k(self, k):
        try:
            int(k)
            return True, None
        except Exception:

            return False, build_error("k parameter cannot be converted to int", 400)

    def validate_bool(self, string_value):
        if string_value is not None:
            if string_value.lower() == "true":
                return True, True
            elif string_value.lower() == "false":
                return True, False
            else:
                return False, build_error("Bool parameter cannot be converted", 400)
        else:
            return True, False
        
    
    def validate_NERtype(self, NERtype):
        if NERtype is None or len(NERtype) == 0:
            return True, None
        if NERtype not in ["LOC", "ORG", "PERS", "OTHERS"]:
            return False, build_error("NERtype parameter is not valid", 400)
        else:
            return True, NERtype
