import requests
import pandas as pd
import json
import time
from datetime import datetime
from getters import get_companies
from utils import PROJECT_DIR
from tqdm import tqdm

DATA_DIR = f'{PROJECT_DIR}/'

#Upload company data
companies = get_companies(DATA_DIR)

#Create list from Series to populate API queries
company_nos = companies.company_no.tolist()

#Upload API key from main folder
api_key = open(f"{DATA_DIR}/api_key.txt", "r")
api_key = (api_key.read())

#URL for company officers list
base_url = "https://api.company-information.service.gov.uk/company/"

dir_list =[]
status_codes = {}
dir_dump_list = []

request_number = 0

for item in tqdm(company_nos):
    if request_number > 1100:
        print("sleeping")
        time.sleep(300)
        request_number = 0
    else: pass
    response = requests.get(f"{base_url}{item}/officers",auth=(api_key,''))
    request_number = request_number + 1 
    status_codes[item] = {}
    status_codes[item]['status_code'] = response.status_code
    status_codes[item]['timestamp'] = str(datetime.now())
    status_codes[item]['request_number'] = request_number 
    if response.status_code == 429:
        print("429_sleeping")
        time.sleep(300)
        request_number = 0
        continue
    else: pass
    if response.status_code != 200:
        continue
    else: pass
    json_search_result = response.text
    data = json.JSONDecoder().decode(json_search_result)
    for item in data['items']:
        dir_dump_list.append(item)
    for item in data['items']:
        dir_list.append(item['links']['officer']['appointments'])

dir_info = pd.json_normalize(dir_dump_list)
dir_search_statuses = pd.DataFrame.from_dict(status_codes)
dir_search_statuses = dir_search_statuses.T

#Export director info, list and search statuses
dir_info.to_csv(f'{DATA_DIR}/outputs/dir_info.csv', index=False)
dir_search_statuses.to_csv(f'{DATA_DIR}/outputs/dir_search_statuses.csv', index=True)
dir_list = pd.Series(dir_list)
dir_list.to_csv(f'{DATA_DIR}/outputs/dir_list.csv', index=False)