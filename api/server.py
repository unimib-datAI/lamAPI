import traceback
import logging
from flask import Flask, request
from flask_cors import CORS
from flask_restx import Api, Resource, fields, reqparse
from model.data_retrievers.column_analysis import ColumnAnalysis
from model.data_retrievers.labels_retriever import LabelsRetriever
from model.data_retrievers.literal_classifier import LiteralClassifier
from model.data_retrievers.literals_retriever import LiteralsRetriever
from model.data_retrievers.lookup_retriever import LookupRetriever
from model.data_retrievers.ner_recognizer import NERRecognizer
from model.data_retrievers.objects_retriever import ObjectsRetriever
from model.data_retrievers.predicates_retriever import PredicatesRetriever
from model.data_retrievers.types_retriever import TypesRetriever
from model.data_retrievers.sameas_retriever import SameasRetriever
from model.data_retrievers.summary_retriever import SummaryRetriever
from model.params_validator import ParamsValidator
from model.utils import build_error
from model.database import Database


database = Database()

# instance objects
params_validator = ParamsValidator()
type_retriever = TypesRetriever(database)
objects_retriever = ObjectsRetriever(database)
predicates_retriever = PredicatesRetriever(database)
labels_retriever = LabelsRetriever(database)
literal_classifier = LiteralClassifier()
literals_retriever = LiteralsRetriever(database)
sameas_retriever = SameasRetriever(database)
lookup_retriever = LookupRetriever(database)
column_analysis_classifier = ColumnAnalysis()
ner_recognition = NERRecognizer()
summary_retriever = SummaryRetriever(database)


def init_services():
    with open("data.txt") as f:
        description = f.read()
    app = Flask("LamAPI")
    CORS(app)
    # Configure logging
    logging.basicConfig(level=logging.DEBUG)
    app.logger.setLevel(logging.DEBUG)
    api = Api(app, version="1.0", title="LamAPI", description=description)

    namespaces = {
        "info": api.namespace("info"),
        "entity": api.namespace(
            "entity", description="Services to perform computations and retrieve additional data about entities."
        ),
        "lookup": api.namespace("lookup", description="Services to perform searches based on an input string."),
        "sti": api.namespace("sti", description="Services to perform tasks related to Semantic Table Interpretation."),
        "classify": api.namespace("classify", description="Services to perform string categorisation."),
        "summary": api.namespace("summary", description="Services to get summary statiscs about the datasets."),
    }

    return app, api, namespaces


app, api, namespaces = init_services()

info = namespaces["info"]
entity = namespaces["entity"]
lookup = namespaces["lookup"]
sti = namespaces["sti"]
classify = namespaces["classify"]
summary = namespaces["summary"]

fields_predicates = info.model(
    "Predicates", {"json": fields.List(fields.List(fields.String), example=[["Q30", "Q60"], ["Q166262", "Q25191"]])}
)

fields_objects = info.model("Objects", {"json": fields.List(fields.String, example=["Q30", "Q166262"])})

fields_sameas = info.model("SameAS", {"json": fields.List(fields.String, example=["Q30", "Q31"])})

fields_literals = info.model("Literals", {"json": fields.List(fields.String, example=["Q30", "Q31"])})

fields_types = info.model("Concepts", {"json": fields.List(fields.String, example=["Q30", "Q31"])})

fields_literal_recognizer = info.model(
    "LiteralRecognizer",
    {
        "json": fields.List(
            fields.String,
            example=[
                "50",
                "12/11/1997",
                "https://www.unimib.it/",
                "mario.rossi@gmail.it",
                "Mount Blanc is located in Aosta Valley",
            ],
        )
    },
)

fields_labels = info.model("Labels", {"json": fields.List(fields.String, example=["Q30", "Q31"])})

fields_rdf2vec = info.model("RDF2Vec", {"json": fields.List(fields.String, example=["Q30", "Q31"])})

fields_column_analysis = info.model(
    "ColumnAnalysis",
    {
        "json": fields.List(
            fields.List(fields.String),
            example=[
                ["10", "100", "1000"],
                ["12/11/1997", "26/08/1997", "14/05/2016"],
                ["London", "New York", "Paris"],
            ],
        )
    },
)

fields_ner = info.model(
    "NERRecognizer",
    {
        "json": fields.List(
            fields.String,
            example=["Albert Einstein was a German Scientist", "Alan Turing was an English Mathematician"],
        )
    },
)

fields_cells = api.model(
    "Cells", {"cells": fields.List(fields.String(), required=True, example=["Rome", "Paris", "Praga"])}
)


@info.route("")
@api.doc(responses={200: "OK"}, description="Infos about this endpoint")
class Info(Resource):
    def get(self):
        info_obj = {
            "title": "LamAPI",
            "description": "This is an API which retrieves data about entities in different Knowledge Graphs and performs entity linking task.",
            "license": {"name": "Apache 2.0", "url": "https://www.apache.org/licenses/LICENSE-2.0.html"},
            "version": "1.0.0"
        }
        return info_obj, 200


class BaseEndpoint(Resource):

    def validate_and_get_json_format(self):
        try:
            data = request.get_json()["json"]
        except:
            return False, build_error("Invalid json format", 400)
        return True, data


@lookup.route("/entity-retrieval")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    params={
        "name": "Name to look for (e.g., Batman Begins).",
        "limit": "The number of entities to be retrieved. The default value is 1000.",
        "kind": "Kind of Named Entity to be matched. Available values: <code>entity</code>, <code>disambiguation</code>, <code>type</code> and <code>predicate</code>.",
        "NERtype": "Type of Named Entity to be matched. Available values: <code>LOC</code>, <code>ORG</code>, <code>PERS</code> and <code>OTHERS</code>.",
        "kg": "The Knowledge Graph to query. Available values: <code>wikidata</code>. Default is <code>wikidata</code>.",
        "fuzzy": "Set this param to True if fuzzy search must be applied. Default is <code>False</code>.",
        "types": "Types to be matched in the Knowledge Graph as constraint in the retrieval. Add Types separeted by spaces. E.g. Scientist Philosopher Person",
        "ids": "Ids of the entity",
        "language": "Language to filter the labels. For example, <code>en</code> for English. Default is <code>None</code>.",
        "query": "Query to be used to test elastic search. Default is <code>None</code>.",
        "cache": "Set this param to True if you want to use the cached result of the search. Default is <code>True</code>.",
        "token": "Private token to access the API."
    },
    description="Given a string as input, the endpoint performs a search in the specified Knowledge Graph.",
)
class Lookup(BaseEndpoint):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("name", type=str, location="args")
        parser.add_argument("limit", type=int, location="args")
        parser.add_argument("token", type=str, location="args")
        parser.add_argument("kind", type=str, location="args")
        parser.add_argument("NERtype", type=str, location="args")
        parser.add_argument("kg", type=str, location="args")
        parser.add_argument("fuzzy", type=str, location="args")
        parser.add_argument("types", type=str, location="args")
        parser.add_argument("ids", type=str, location="args")
        parser.add_argument("language", type=str, location="args")
        parser.add_argument("query", type=str, location="args")
        parser.add_argument("cache", type=str, location="args")
        args = parser.parse_args()

        name = args["name"]
        limit = args["limit"]
        token = args["token"]
        kg = args["kg"]
        fuzzy = args["fuzzy"]
        types = args["types"]
        kind = args["kind"]
        NERtype = args["NERtype"]
        language = args["language"]
        ids = args["ids"]
        query = args["query"]
        cache = args["cache"] if args["cache"] is not None else True

        token_is_valid, token_error = params_validator.validate_token(token)
        if not token_is_valid:
            return token_error

        is_fuzzy_valid, fuzzy_value = params_validator.validate_bool(fuzzy)

        if not is_fuzzy_valid:
            return fuzzy_value

        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)
        if not kg_is_valid:
            return kg_error_or_value

        limit_is_valid, limit_error_or_value = params_validator.validate_limit(limit)
        if not limit_is_valid:
            return limit_error_or_value

        if name is None:
            return build_error("Name is required", 400)

        try:
            results = lookup_retriever.search(
                name=name,
                limit=limit_error_or_value,
                kg=kg_error_or_value,
                fuzzy=fuzzy_value,
                types=types,
                kind=kind,
                NERtype=NERtype,
                language=language,
                ids=ids,
                query=query,
                cache=cache
            )
        except Exception as e:
            print("Error", e, flush=True)
            return build_error(str(e), 400, traceback=traceback.format_exc())

        return results


@entity.route("/types")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input composed of Wikidata entities, the endpoint returns the associated TYPES for each entity.",
    params={
        "kg": "The Knowledge Graph to query. Available values: <code>wikidata</code>. Default is <code>wikidata</code>.",
        "token": "Private token to access the APIs."
    },
)
class Types(BaseEndpoint):
    @entity.doc(body=fields_types)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        parser.add_argument("kg", type=str)
        args = parser.parse_args()

        token = args["token"]
        kg = args["kg"]
        token_is_valid, token_error = params_validator.validate_token(token)
        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)

        if not token_is_valid:
            return token_error
        elif not kg_is_valid:
            return kg_error_or_value
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return type_retriever.get_types_output(data, kg_error_or_value)
            else:
                return build_error("Invalid Data", 400)


@entity.route("/objects")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input composed of DBPedia or Wikidata entities, the endpoint returns a list of OBJECTS for each entity.",
    params={
        "token": "Private token to access the APIs.",
        "kg": "The Knowledge Graph to query. Available values: <code>wikidata</code>. Default is <code>wikidata</code>.",
    },
)
class Objects(BaseEndpoint):
    @entity.doc(body=fields_objects)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        parser.add_argument("kg", type=str)
        args = parser.parse_args()

        token = args["token"]
        kg = args["kg"]

        token_is_valid, token_error = params_validator.validate_token(token)
        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)

        if not token_is_valid:
            return token_error
        elif not kg_is_valid:
            return kg_error_or_value
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                try:
                    result = objects_retriever.get_objects_output(data, kg_error_or_value)
                    return result
                except Exception:
                    print(traceback.format_exc())
            else:
                print("objects invalid", data, flush=True)
                return build_error("Invalid Data", 400, traceback=traceback.format_exc())


@entity.route("/predicates")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input composed of Wikidata entities, the endpoint returns a list of PREDICATES between each pair of entities (SUBJECT and OBJECT).",
    params={
        "kg": "The Knowledge Graph to query. Available values: <code>wikidata</code>. Default is <code>wikidata</code>.",
        "token": "Private token to access the APIs."
    },
)
class Predicates(BaseEndpoint):
    @entity.doc(body=fields_predicates)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        parser.add_argument("kg", type=str)
        args = parser.parse_args()

        token = args["token"]
        kg = args["kg"]

        token_is_valid, token_error = params_validator.validate_token(token)
        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)

        if not token_is_valid:
            return token_error
        elif not kg_is_valid:
            return kg_error_or_value
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return predicates_retriever.get_predicates_output(data, kg_error_or_value)
            else:
                return build_error("Invalid Data", 400)


@entity.route("/labels")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input composed of DBpedia or Wikidata entities, the endpoint returns a list of LABELS and ALIASES for each entity. It's also possible to specify the language to filter the labels.",
    params={
        "kg": "The Knowledge Graph to query. Available values: <code>wikidata</code>. Default is <code>wikidata</code>.",
        "lang": "Language to filter the labels.",
        "token": "Private token to access the APIs."
    },
)
class Labels(BaseEndpoint):
    @entity.doc(body=fields_labels)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        parser.add_argument("kg", type=str)
        parser.add_argument("lang", type=str)
        args = parser.parse_args()

        token = args["token"]
        kg = args["kg"]
        lang = args["lang"]

        token_is_valid, token_error = params_validator.validate_token(token)
        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)

        if not token_is_valid:
            return token_error
        elif not kg_is_valid:
            return kg_error_or_value
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return labels_retriever.get_labels_output(data, kg_error_or_value, lang)
            else:
                return build_error("Invalid Data", 400)


@entity.route("/sameas")
@api.doc(
    description="Given a JSON array as input composed of Wikidata entities, the endpoint returns the associated entities in Wikipedia and DBpedia.",
    params={"token": "Private token to access the API."},
)
class SameAs(BaseEndpoint):
    @entity.doc(body=fields_sameas)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        args = parser.parse_args()

        token = args["token"]

        token_is_valid, token_error = params_validator.validate_token(token)

        if not token_is_valid:
            return token_error
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return sameas_retriever.get_sameas_output(data)
            else:
                build_error("Invalid Data", 400)


@classify.route("/literal-recognizer")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="""
    Given a JSON array as input composed of a set of strings, the endpoint returns the types of literal. The list of literals recognized is:
    **DATE**:
    * '145 bc', '145.bc', '145,bc'
    * '1997-08-26', '1997.08.26', '1997/08/26'
    * '26/08/1997', '26.08.1997', '26-08-1997'
    * '26/08/97', '26.08.97', '26-08-97'
    * 'august 26 1997', 'august.26.1997', 'august,26,1997'
    * '26 august 1997', '26.august.1997', '26,august,1997'
    * '1997 august 26', '1997,august,26', '1997.august.26'
    * '1997 26 august', '1997,26,august', '1997.26.august'
    * 'august 1997', 'august.1997', 'august,1997'
    * '1997 august', '1997.august', '1997,august'
    * 1997-2022, 1997-present, 1997-now
    **NUMBERS**:
    * '2,797,800,564', '2.797.800.564'
    * '200,797,800', '200.797.800'
    * 1997, 1345, 26, 1
    * +/- 34, +/- 34657
    * 25 thousand, 25 million, 25 billion, 25 trillion
    * 2 km, 2 km2, 2 cm, 2 cm2, 2 mm, 2 mm2, 10 sq, 10 ft, 10 dm,....
    * 2,8, 2.8
    * +/- 5e+/-10
    **OTHERS**:
    * https://elearning.unimib.it/
    * mario.rossi@gmail.com
    * 12pm
    * 12.30pm
    * 12:30pm
    * 2.30 am
    """,
    params={"token": "Private token to access the APIs."},
)
class LiteralRecognizer(BaseEndpoint):
    @classify.doc(body=fields_literal_recognizer)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        args = parser.parse_args()

        token = args["token"]

        token_is_valid, token_error = params_validator.validate_token(token)

        if not token_is_valid:
            return token_error
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return literal_classifier.classifiy_literal(data)
            else:
                build_error("Invalid Data", 400)


@entity.route("/literals")
@entity.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input made of DBpedia or Wikipedia entities, the endpoint returns the list of LITERALS classified as DATETIME, NUMBER or STRING for each entity.",
    params={
        "kg": "The Knowledge Graph to query. Available values: <code>wikidata</code>. Default is <code>wikidata</code>.",
        "token": "Private token to access the APIs.",
    },
)
class Literals(BaseEndpoint):
    @entity.doc(body=fields_literals)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        parser.add_argument("kg", type=str)
        args = parser.parse_args()

        token = args["token"]
        kg = args["kg"]

        token_is_valid, token_error = params_validator.validate_token(token)
        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)

        if not token_is_valid:
            return token_error
        elif not kg_is_valid:
            return kg_error_or_value
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return literals_retriever.get_literals_output(data, kg_error_or_value)
            else:
                build_error("Invalid Data", 400)


@sti.route("/column-analysis")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input composed of a set of array of strings (cell content), the endpoint calculates, for each array, if the content represents named-entitites or literals.",
    params={"token": "Private token to access the APIs."},
)
class ColumnAnalysis(BaseEndpoint):
    @api.doc(body=fields_column_analysis)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        args = parser.parse_args()

        token = args["token"]

        token_is_valid, token_error = params_validator.validate_token(token)

        if not token_is_valid:
            return token_error
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                result = column_analysis_classifier.classify_columns(columns=data)
                return result
            else:
                build_error("Invalid Data", 400)


@classify.route("/name-entity-recognition")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    description="Given a JSON array as input composed of a set of array of natural language, the endpoint performs the task of Name Entity Recogition and returns the list of mentions found i the text.",
    params={"token": "Private token to access the APIs."},
)
class NERAnalysis(BaseEndpoint):
    @api.doc(body=fields_ner)
    def post(self):
        # get parameters
        parser = reqparse.RequestParser()
        parser.add_argument("token", type=str)
        args = parser.parse_args()

        token = args["token"]

        token_is_valid, token_error = params_validator.validate_token(token)

        if not token_is_valid:
            return token_error
        else:
            is_data_valid, data = super().validate_and_get_json_format()
            if is_data_valid:
                return ner_recognition.recognize_entities(data)
            else:
                build_error("Invalid Data", 400)


@summary.route("/")
@api.doc(
    responses={200: "OK", 404: "Not found", 400: "Bad request", 403: "Invalid token"},
    params={
        "kg": "The Knowledge Graph to query. Values: 'wikidata'. Default is 'wikidata'.",
        "data_type": "Type of data to retrieve. Values: 'objects' or 'literals'.",
        "rank_order": "Order of the results based on rank of predicates. Values: 'desc' or 'asc'.",
        "k": "Number of elements to return when ordering by rank. Default is 10.",
        "token": "Private token to access the API.",
    },
    description="Returns the summary of the specified Knowledge Graph with optional ordering by rank of predicates.",
)
class Summary(BaseEndpoint):
    def get(self):
        parser = reqparse.RequestParser()
        parser.add_argument("kg", type=str, location="args", default="wikidata")
        parser.add_argument("data_type", type=str, location="args", default="objects")
        parser.add_argument("rank_order", type=str, location="args")
        parser.add_argument("k", type=int, location="args")
        parser.add_argument("token", type=str, location="args")
        args = parser.parse_args()

        kg = args["kg"]
        data_type = args["data_type"]
        rank_order = args["rank_order"]
        k = args["k"]
        token = args["token"]

        # Validate token
        token_is_valid, token_error = params_validator.validate_token(token)
        if not token_is_valid:
            return token_error

        # Validate KG
        kg_is_valid, kg_error_or_value = params_validator.validate_kg(database, kg)
        if not kg_is_valid:
            return build_error("Invalid KG. Use 'wikidata'", 400)

        # Validate rank order
        if rank_order and rank_order not in ["asc", "desc"]:
            return build_error("Invalid rank order. Use 'asc' or 'desc'.", 400)

        # If k is not provided, use default value
        k = k if k is not None else 10

        # Implement the logic to retrieve Wikidata or DBpedia summary based on parameters
        if data_type == "objects":
            results = summary_retriever.get_objects_summary(kg=kg_error_or_value, rank_order=rank_order, k=k)
        elif data_type == "literals":
            results = summary_retriever.get_literals_summary(kg=kg_error_or_value, rank_order=rank_order, k=k)
        else:
            return build_error("Invalid data type. Use 'objects' or 'literals'.", 400)

        return results
