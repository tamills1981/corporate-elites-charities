import requests
import pandas as pd
import json
import time
from datetime import datetime
from tqdm import tqdm
from getters import get_corporate_elite
from utils import PROJECT_DIR

DATA_DIR = f'{PROJECT_DIR}/'

#Upload company data
corporate_elite = get_corporate_elite(DATA_DIR)

#Create list from Series to populate API queries
director_ids = (
    corporate_elite['director_id'].drop_duplicates() 
    .tolist()
    )

#Upload API key from main folder
api_key = open(f"{DATA_DIR}/api_key.txt", "r")
api_key = (api_key.read())

#URL for company officers list
base_url = "https://api.company-information.service.gov.uk/"

dir_list = []
status_codes = {}

request_number = 0

for director_id in tqdm(director_ids):
    if request_number > 1100:
        print("sleeping")
        time.sleep(300)
        request_number = 0
    else: pass
    search_term = '/officers/' + director_id + '/appointments'
    response = requests.get(f"{base_url}{search_term}/",auth=(api_key,''))
    request_number = request_number + 1 
    status_codes[director_id] = {}
    status_codes[director_id]['status_code'] = response.status_code
    status_codes[director_id]['timestamp'] = str(datetime.now())
    status_codes[director_id]['request_number'] = request_number 
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
    dir_info = pd.json_normalize(data['items'])
    dir_info.insert(loc = 0,
              column = 'director_id',
              value = director_id)
    dir_list.append(dir_info)

status_codes = pd.DataFrame.from_dict(status_codes)
status_codes = status_codes.T

corporate_elite_appointments = pd.concat(dir_list, ignore_index=True)

#Export appointments data and search statuses as csvs
corporate_elite_appointments.to_csv(f'{DATA_DIR}/outputs/corporate_elite_appointments.csv', index=False)
status_codes.to_csv(f'{DATA_DIR}/outputs/corproate_elite_appointment_search_statuses.csv', index=True)