import bz2
import json
from tqdm import tqdm
import traceback
import os
from pymongo import MongoClient

from json.decoder import JSONDecodeError