import os
import pandas as pd
from tqdm import tqdm
import logging
import requests
from SPARQLWrapper import SPARQLWrapper, JSON
import time
import re
import json
import aiohttp
import asyncio
import backoff
import nest_asyncio

R1_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_Round1/R1_NER_query_type.json"

with open(R1_json_file_path, "r") as file:
    R1_ner_type = json.load(file)

R1_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_Round1/R1_WD_query_type.json"

with open(R1_json_file_path, "r") as file:
    R1_explicit_type = json.load(file)

R1_tables_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round1_T2D/tables/"
R1_cea_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round1_T2D/gt/CEA_Round1_gt_WD.csv'
R1_cta_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round1_T2D/gt/CTA_Round1_gt.csv'
################################

R3_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_Round3/R3_NER_query_type.json"

with open(R3_json_file_path, "r") as file:
    R3_ner_type = json.load(file)

R3_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_Round3/R3_WD_query_type.json"

with open(R3_json_file_path, "r") as file:
    R3_explicit_type = json.load(file)

R3_tables_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round3_2019/tables/"
R3_cea_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round3_2019/gt/CEA_Round3_gt_WD.csv'
R3_cta_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round3_2019/gt/CTA_Round3_gt.csv'
##############################

R4_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_Round4/R4_NER_query_type.json"

with open(R4_json_file_path, "r") as file:
    R4_ner_type = json.load(file)

R4_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_Round4/R4_WD_query_type.json"

with open(R4_json_file_path, "r") as file:
    R4_explicit_type = json.load(file)

R4_tables_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round4_2020/tables/"
R4_cea_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round4_2020/gt/cea.csv'
R4_cta_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/Round4_2020/gt/cta.csv'
##############################

HTR2_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_HTR2/HTR2_NER_query_type.json"

with open(HTR2_json_file_path, "r") as file:
    HTR2_ner_type = json.load(file)

HTR2_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_HTR2/HTR2_WD_query_type.json"

with open(HTR2_json_file_path, "r") as file:
    HTR2_explicit_type = json.load(file)

HTR2_tables_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/HardTablesR2/tables/"
HTR2_cea_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/HardTablesR2/gt/cea.csv'
HTR2_cta_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/HardTablesR2/gt/cta.csv'
##############################

HTR3_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_HTR3/HTR3_NER_query_type.json"

with open(HTR3_json_file_path, "r") as file:
    HTR3_ner_type = json.load(file)

HTR3_json_file_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/work/_HTR3/HTR3_WD_query_type.json"

with open(HTR3_json_file_path, "r") as file:
    HTR3_explicit_type = json.load(file)

HTR3_tables_path = "C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/HardTablesR3/tables/"
HTR3_cea_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/HardTablesR3/gt/cea.csv'
HTR3_cta_file = 'C:/ALESSANDRO/Università/MAGISTRALE/SINTEF_thesis/lamAPI/data/Dataset/Dataset/HardTablesR3/gt/cta.csv'
#############################

# Define lists of table paths and CEA files
tables_paths = [R1_tables_path, R3_tables_path, R4_tables_path, HTR2_tables_path, HTR3_tables_path]
cea_files = [R1_cea_file, R3_cea_file, R4_cea_file, HTR2_cea_file, HTR3_cea_file]
prefix = ['R1', 'R3', 'R4', 'HTR2', 'HTR3']


# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to read CEA file and create dictionaries
def read_cea_file(cea_file):
    df_cea = pd.read_csv(cea_file, header=None)
    df_cea["key"] = df_cea[0] + " " + df_cea[1].astype(str) + " " + df_cea[2].astype(str)
    df_cea["key_col"] = df_cea[0] + " " + df_cea[2].astype(str)
    cea_values_dict = dict(zip(df_cea["key_col"].values, df_cea[3].values))
    cea_values_dict_cell = dict(zip(df_cea["key"].values, df_cea[3].values))
    cea_keys_set = set(df_cea["key"].values)
    return cea_keys_set, cea_values_dict, cea_values_dict_cell

# Function to process a single table file
def process_table_file(table_file, cea_keys_set, cea_values_dict_cell):
    try:
        table_name = os.path.splitext(os.path.basename(table_file))[0]
        df = pd.read_csv(table_file)
        qid_to_value = {}

        for row in range(df.shape[0]):
            for col in range(df.shape[1]):
                key = f"{table_name} {row+1} {col}"
                if key in cea_keys_set:
                    cell_value = df.iloc[row, col]
                    qid = cea_values_dict_cell[key].split('/')[-1]  # Extract the QID from the URL
                    qid_to_value[cell_value] = qid
                    break  # Exit inner loop early as only one match per row/col is needed
        
        return qid_to_value
    except Exception as e:
        logging.error(f"Error processing {table_file}: {e}")
        return {}

# Process each table path and corresponding CEA file
id_to_name_dicts = {}

for tables_path, cea_file, name in zip(tables_paths, cea_files, prefix):
    cea_keys_set, cea_values_dict, cea_values_dict_cell = read_cea_file(cea_file)

    #########################################
    if tables_path == R4_tables_path:
        break
    #########################################
    
    # List of table files in the directory
    table_files = [
        os.path.join(tables_path, table)
        for table in os.listdir(tables_path)
        if not table.startswith('.')
    ]
    
    # Initialize dictionary for this prefix
    id_to_name_dicts[name + "_id_to_name"] = {}
    
    for table_file in tqdm(table_files, desc=f"Processing tables for {name}"):
        local_key_to_cell = process_table_file(table_file, cea_keys_set, cea_values_dict_cell)
        id_to_name_dicts[name + "_id_to_name"].update(local_key_to_cell)


def get_hard_query_ner_to_ner(name, value):
    name = str(name).replace('"', ' ')
    if value is not None:

        query_dict = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": {"query": name, "boost": 2.0}}},
                        {"terms": {"NERtype": value}}  # Ensures `value` matches at least one in the array
                    ]
                }
            }
        }
        
        params = {
            'name': name,
            'token': 'lamapi_demo_2023',
            'kg': 'wikidata',
            'limit': 100,
            'query': json.dumps(query_dict),  # Convert the query dictionary to a JSON string
            'sort': [
                '{"popularity": {"order": "desc"}}'
            ]
        }
    
    return params

def get_soft_query_ner_to_ner(name, value):
    name = str(name).replace('"', ' ')  # Replace double quotes with spaces

    should_clause = []
    if value:
        if isinstance(value, list):
            should_clause = [{"term": {"NERtype": value}}]
        else:
            should_clause = [{"term": {"NERtype": value}}]

    query_dict = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"name": {"query": name, "boost": 2.0}}}
                ],
                "should": should_clause
            }
        }
    }

    params = {
        'name': name,
        'token': 'lamapi_demo_2023',
        'kg': 'wikidata',
        'limit': 100,
        'query': json.dumps(query_dict),  # Compact JSON
        'sort': ['{"popularity": {"order": "desc"}}']
    }

    return params


#candidate_types_ner = [R1_ner_type, R3_ner_type, R4_ner_type, HTR2_ner_type, HTR3_ner_type]
candidate_types_ner = [R1_ner_type, R3_ner_type]
prefix = ['R1', 'R3']
queries_ner_to_ner_HARD = {}
for (file_name, id_name), db_name, ner_type in zip(tqdm(id_to_name_dicts.items()), prefix, candidate_types_ner):
    tmp_query = []
    for name, id in tqdm(id_name.items(), desc = f"HARD ner_to_ner: processing {file_name}"):
        if id in ner_type:
            types_list = ner_type[id]      
            query = get_hard_query_ner_to_ner(name, types_list)
            tmp_query.append((query, id, types_list))
    queries_ner_to_ner_HARD[db_name] = tmp_query


queries_ner_to_ner_SOFT = {}
for (file_name, id_name), db_name, ner_type in zip(tqdm(id_to_name_dicts.items()), prefix, candidate_types_ner):
    tmp_query = []
    for name, id in tqdm(id_name.items(), desc = f"SOFT ner_to_ner: processing {file_name}"):
        if id in ner_type:
            types_list = ner_type[id]      
            query = get_soft_query_ner_to_ner(name, types_list)
            tmp_query.append((query, id, types_list))
    queries_ner_to_ner_SOFT[db_name] = tmp_query


def get_hard_query_explicit_to_extended(name, value):
    name = str(name).replace('"', ' ')
    if value is not None:

        query_dict = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": {"query": name, "boost": 2.0}}},
                        {"terms": {"extended_WDtypes": value}}  # Ensures `value` matches at least one in the array
                    ]
                }
            }
        }
        
        params = {
            'name': name,
            'token': 'lamapi_demo_2023',
            'kg': 'wikidata',
            'limit': 100,
            'query': json.dumps(query_dict),  # Convert the query dictionary to a JSON string
            'sort': [
                '{"popularity": {"order": "desc"}}'
            ]
        }
    
    return params

def get_soft_query_explicit_to_extended(name, value):
    name = str(name).replace('"', ' ')  # Replace double quotes with spaces

    should_clause = []
    if value:
        if isinstance(value, list):
            should_clause = [{"term": {"extended_WDtypes": v}} for v in value]
        else:
            should_clause = [{"term": {"extended_WDtypes": value}}]

    query_dict = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"name": {"query": name, "boost": 2.0}}}
                ],
                "should": should_clause
            }
        }
    }

    params = {
        'name': name,
        'token': 'lamapi_demo_2023',
        'kg': 'wikidata',
        'limit': 100,
        'query': json.dumps(query_dict),  # Compact JSON
        'sort': ['{"popularity": {"order": "desc"}}']
    }

    return params


#candidate_types_ner = [R1_explicit_type, R3_explicit_type, R4_explicit_type, HTR2_explicit_type, HTR3_explicit_type]
candidate_types_explicit = [R1_explicit_type, R3_explicit_type]
prefix = ['R1', 'R3']
queries_explicit_to_extended_HARD = {}
for (file_name, id_name), db_name, explicit_type in zip(tqdm(id_to_name_dicts.items()), prefix, candidate_types_explicit):
    tmp_query = []
    for name, id in tqdm(id_name.items(), desc = f"HARD explicit_to_extended: processing {file_name}"):
        if id in explicit_type:
            types_list = explicit_type[id]      
            query = get_hard_query_explicit_to_extended(name, types_list)
            tmp_query.append((query, id, types_list))
    queries_explicit_to_extended_HARD[db_name] = tmp_query


queries_explicit_to_extended_SOFT = {}
for (file_name, id_name), db_name, explicit_type in zip(tqdm(id_to_name_dicts.items()), prefix, candidate_types_explicit):
    tmp_query = []
    for name, id in tqdm(id_name.items(), desc = f"SOFT explicit_to_extended: processing {file_name}"):
        if id in explicit_type:
            types_list = explicit_type[id]      
            query = get_soft_query_explicit_to_extended(name, types_list)
            tmp_query.append((query, id, types_list))
    queries_explicit_to_extended_SOFT[db_name] = tmp_query



def get_hard_query_ner_to_extended(name, value):
    name = str(name).replace('"', ' ')
    if value is not None:

        query_dict = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": {"query": name, "boost": 2.0}}},
                        {"terms": {"extended_WDtypes": value}}  # Ensures `value` matches at least one in the array
                    ]
                }
            }
        }
        
        params = {
            'name': name,
            'token': 'lamapi_demo_2023',
            'kg': 'wikidata',
            'limit': 100,
            'query': json.dumps(query_dict),  # Convert the query dictionary to a JSON string
            'sort': [
                '{"popularity": {"order": "desc"}}'
            ]
        }

    else:
        
        query_dict = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": {"query": name, "boost": 2.0}}}
                    ],
                    "must_not": [
                        {"terms": {"extended_WDtypes": ["Q43229", "Q27096213", "Q5"]}}  # Exclude documents mapped to ORG, LOC or PERS (include only OTHERS)
                    ]
                }
            }
        }

        
        params = {
            'name': name,
            'token': 'lamapi_demo_2023',
            'kg': 'wikidata',
            'limit': 100,
            'query': json.dumps(query_dict),  # Convert the query dictionary to a JSON string
            'sort': [
                '{"popularity": {"order": "desc"}}'
            ]
        }
    
    return params

def get_soft_query_ner_to_extended(name, value):
    name = str(name).replace('"', ' ')  # Replace double quotes with spaces

    should_clause = []
    if value:
        if isinstance(value, list):
            should_clause = [{"term": {"extended_WDtypes": v}} for v in value]
        else:
            should_clause = [{"term": {"extended_WDtypes": value}}]
        
        query_dict = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": {"query": name, "boost": 2.0}}}
                    ],
                    "should": should_clause
                }
            }
        }
    else:
        query_dict = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"name": {"query": name, "boost": 2.0}}}
                    ],
                    "should": {
                        "bool": {
                            "must_not": [
                                {"terms": {"extended_WDtypes": ["Q43229", "Q27096213", "Q5"]}}  # Exclude documents mapped to ORG, LOC or PERS (include only OTHERS)
                            ]
                        }
                    }
                }
            }
        }

    params = {
        'name': name,
        'token': 'lamapi_demo_2023',
        'kg': 'wikidata',
        'limit': 100,
        'query': json.dumps(query_dict),  # Compact JSON
        'sort': ['{"popularity": {"order": "desc"}}']
    }

    return params



entity_mapping = {
    'ORG': 'Q43229',
    'LOC': 'Q27096213',
    'PERS': 'Q5',
    'OTHERS':  None    #sistemare qua quello di others ############
}



queries_ner_to_extended_HARD = {}
for (file_name, id_name), db_name, explicit_type in zip(tqdm(id_to_name_dicts.items()), prefix, candidate_types_ner):
    tmp_query = []
    for name, id in tqdm(id_name.items(), desc = f"HARD ner_to_extended: processing {file_name}"):
        if id in ner_type:
            types_list = ner_type[id]  
            mapped_type = entity_mapping.get(types_list)
            query = get_hard_query_ner_to_extended(name, mapped_type)
            tmp_query.append((query, id, types_list))
    queries_ner_to_extended_HARD[db_name] = tmp_query


queries_ner_to_extended_SOFT = {}
for (file_name, id_name), db_name, explicit_type in zip(tqdm(id_to_name_dicts.items()), prefix, candidate_types_ner):
    tmp_query = []
    for name, id in tqdm(id_name.items(), desc = f"SOFT ner_to_extended: processing {file_name}"):
        if id in ner_type:
            types_list = explicit_type[id]      
            query = get_soft_query_ner_to_extended(name, types_list)
            tmp_query.append((query, id, types_list))
    queries_ner_to_extended_SOFT[db_name] = tmp_query




########################################################################################################

failed_queries = {}
url = 'http://localhost:5000/lookup/entity-retrieval'

# Backoff decorator for handling retries with exponential backoff
@backoff.on_exception(
    backoff.expo, 
    (aiohttp.ClientError, aiohttp.http_exceptions.HttpProcessingError, asyncio.TimeoutError), 
    max_tries=10, 
    max_time=400
)
async def fetch(session, url, params, headers, semaphore):
    async with semaphore:
        # Convert all params to str, int, or float
        #params = {k: (int(v) if isinstance(v, np.integer) else str(v)) for k, v in params.items()}
        async with session.get(url, params=params, headers=headers, timeout=50) as response:
            try:
                response.raise_for_status()  # Raises an exception for 4XX/5XX status codes
                return await response.json()
            except asyncio.TimeoutError:
                print(f"Request timed out for params: {params}")
                return []  # Return an empty list to handle the timeout gracefully
            except aiohttp.ClientError as e:
                print(f"ClientError for params : {str(e)}")
                return []
            except Exception as e:
                print(f"Unexpected error for params {params}: {str(e)}")
                return []
async def process_item(session, url, id, headers, params, semaphore, pbar):
    try:
        data = await fetch(session, url, params, headers, semaphore)
    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            print(f"404 Error: Resource not found for '{id}'")
            asyncio.get_event_loop().call_soon_threadsafe(pbar.update, 1)
            return 0, 0
        else:
            raise  # Re-raise the exception for other status codes

    num_result = len(data) if data else 0

    
    #print(f"------------>{eval(params['query'])['query']['bool']['must'][1]} - # candidate: {len(data)}")
    if data:
        for item in data:
            if id == item.get('id'):
                #print(f"{item.get('name')}: es_score({item.get('es_score', 0)}), pos_score({item.get('pos_score', 0)})-> {item.get('description')}")
                asyncio.get_event_loop().call_soon_threadsafe(pbar.update, 1)
                pos_score = item.get('pos_score', 0)
                if pos_score:
                    mrr_increment = (num_result - (pos_score * num_result)) / num_result
                else:
                    mrr_increment = 1 / num_result  # Assume worst case for MRR if pos_score is 0
                return mrr_increment, 1

    return 0, 0

async def main(queries, url, pbar, failed_queries,db_name):
    headers = {'accept': 'application/json'}
    semaphore = asyncio.Semaphore(50)  # Limit to 50 concurrent requests
    m_mrr = 0
    cont_el = 0

    async with aiohttp.ClientSession() as session:
        tasks = []
        for param, id, _ in queries:
            tasks.append(process_item(session, url, id, headers, param, semaphore, pbar))
        
        results = await asyncio.gather(*tasks)
        
        for (mrr_increment, count), (param, id, item_NERtype) in zip(results, queries):
            if mrr_increment == 0 and count == 0:
                failed_queries[id] = (id, item_NERtype)
                
                # redo the same query with the fuzzy
                name = param['name']
                
                # Parse the string into a Python dictionary
                query_dict = json.loads(param['query'])

                # Modify the "match" field
                if "query" in query_dict and "bool" in query_dict["query"] and "must" in query_dict["query"]["bool"]:
                    for condition in query_dict["query"]["bool"]["must"]:
                        if "match" in condition and "name" in condition["match"]:
                            condition["match"]["name"]["fuzziness"] = "AUTO"

                # Convert back to JSON string
                param['query'] = json.dumps(query_dict)
                print(param['query'])

                response = requests.get(url, params=param)
                if response.status_code == 200:
                    data = response.json()
                    #print("after call")
                    num_result = len(data) if data else 0
                    if data:
                        for item in data:
                            if id == item.get('id'):
                                pbar.update(1)  # No need to await here
                                pos_score = item.get('pos_score', 0)
                                if pos_score:
                                    mrr_increment = (num_result - (pos_score * num_result)) / num_result
                                else:
                                    mrr_increment = 1 / num_result  # Assume worst case for MRR if pos_score is 0
                            
                m_mrr += mrr_increment
                cont_el += count 
            else:
                m_mrr += mrr_increment
                cont_el += count

        asyncio.get_event_loop().call_soon_threadsafe(pbar.close)

    print(f"Coverage of {db_name}: {cont_el / len(queries)}")
    print(f"Measure Reciprocal Rank of {db_name}: {m_mrr / len(queries)}")



queries_HARD = [queries_ner_to_ner_HARD, queries_explicit_to_extended_HARD, queries_ner_to_extended_HARD]
queries_SOFT = [queries_ner_to_ner_SOFT, queries_explicit_to_extended_SOFT, queries_ner_to_extended_SOFT]

for el in queries_HARD:
    for db_name,queries in el.items():
        nest_asyncio.apply()  # Apply nest_asyncio
        try:
            pbar = tqdm(total=len(queries), desc=f"processing queries for {db_name}")
            asyncio.run(main(queries, url, pbar, failed_queries,db_name))
        except RuntimeError:  # For environments like Jupyter
            loop = asyncio.get_event_loop()
            loop.run_until_complete(main(queries, url, pbar, failed_queries))
