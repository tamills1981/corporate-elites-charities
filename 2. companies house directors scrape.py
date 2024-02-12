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

company_nos = company_nos[0:50]

#Upload API key from main folder
api_key = open(f"{DATA_DIR}/api_key.txt", "r")
api_key = (api_key.read())

#URL for company officers list
base_url = "https://api.company-information.service.gov.uk/company/"

#Empty lists and dictionary to populate 
dir_list =[]
dir_data_list = []
status_codes = {}

request_number = 0
items_per_page = 35

for company_no in tqdm(company_nos):
    start_index = 0
    total_results = float('inf')

    while start_index < total_results:
        if request_number > 1100:
            print("sleeping")
            time.sleep(300)
            request_number = 0
        else:
            pass

        response = requests.get(f"{base_url}{company_no}/officers?start_index={start_index}&items_per_page={items_per_page}", auth=(api_key, ''))
        request_number += 1

        status_codes[company_no] = {
            'page_number': start_index,
            'status_code': response.status_code,
            'timestamp': str(datetime.now()),
            'request_number': request_number
        }

        if response.status_code != 200:
            break

        data = json.loads(response.text)
        total_results = data['total_results']

        for item in data['items']:
            dir_data_list.append(item)
            dir_list.append(item['links']['officer']['appointments'])

        start_index += items_per_page

dir_info = pd.json_normalize(dir_data_list)
dir_search_statuses = pd.DataFrame.from_dict(status_codes)
dir_search_statuses = dir_search_statuses.T

print(dir_search_statuses['status_code'].value_counts())

#Export director info, list and search statuses
dir_info.to_csv(f'{DATA_DIR}/outputs/dir_info.csv', index=False)
dir_search_statuses.to_csv(f'{DATA_DIR}/outputs/dir_search_statuses.csv', index=True)
dir_list = pd.Series(dir_list)
dir_list.to_csv(f'{DATA_DIR}/outputs/dir_list.csv', index=False)