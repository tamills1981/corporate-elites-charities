import requests
import pandas as pd
import time
from datetime import datetime
from getters import get_charities
from utils import PROJECT_DIR
from tqdm import tqdm

DATA_DIR = f'{PROJECT_DIR}/'

TEST = False

#Upload charities data
charities = get_charities(DATA_DIR)

#Create smaller dataframe of names and company ids
charities = charities[['charity_name', 'charity_company_registration_number']].dropna()

#Fix company numbers
charities['charity_company_registration_number'] = (
               charities['charity_company_registration_number']
               .astype(str)
               .str.replace('.0','')
               .str.zfill(8)
               .to_list()
               )

#Convert into list of tuples
list_of_tuples = list(charities.itertuples(index=False, name=None))

if TEST: list_of_tuples = list_of_tuples[0:10]

#Upload API key from main folder
api_key = open(f"{DATA_DIR}/api_key.txt", "r")
api_key = (api_key.read())

#URL for query
base_url = "https://api.company-information.service.gov.uk/company/"

#Dictionary to populate with response status codes
status_codes = {}

request_number = 0

for item in tqdm(list_of_tuples):
        if request_number > 1100:
            print("sleeping")
            time.sleep(300)
            request_number = 0
        else:
            pass
        
        company_no = item[1]
        response = requests.get(f"{base_url}{company_no}", auth=(api_key, ''))
        
        status_codes[company_no] = {
            'charity_name': item[0],
            'status_code': response.status_code,
            'timestamp': str(datetime.now()),
            'request_number': request_number
        }

        request_number += 1

#Create df from status codes list and print results summary
search_statuses = pd.DataFrame.from_dict(status_codes)
search_statuses = search_statuses.T
search_statuses = search_statuses.reset_index(names='charity_company_registration_number')
print(search_statuses['status_code'].value_counts())

#Create small df of company numbers returning 404 responses
filt = search_statuses['status_code'] == 404 
erroneous_co_numbers = search_statuses.loc[filt]

#Export dataframes as csvs
search_statuses.to_csv(f'{DATA_DIR}/outputs/charity_co_no_check_search_statuses.csv', index=True)
erroneous_co_numbers.to_csv(f'{DATA_DIR}/outputs/erroneous_co_numbers.csv', index=False)