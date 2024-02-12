import requests
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
from getters import get_corporate_elite
from utils import PROJECT_DIR

DATA_DIR = f'{PROJECT_DIR}/'

#Upload corporate elite data
corporate_elite = get_corporate_elite(DATA_DIR)

#Create list if director ids to populate API queries
director_ids = (
    corporate_elite['director_id'].drop_duplicates() 
    .tolist()
    )

#Upload API key from main folder
api_key = open(f"{DATA_DIR}/api_key.txt", "r")
api_key = (api_key.read())

#URL for individuals' appointments
base_url = "https://api.company-information.service.gov.uk/"

#Empty list and dictionary to populate 
dir_list = []
status_codes = {}

request_number = 0

for director_id in tqdm(director_ids):
    items_per_page = 35
    start_index = 0
    total_results = float('inf')
    
    while start_index < total_results:
        query_text = '/officers/' + director_id + '/appointments'
        response = requests.get(f'{base_url}{query_text}/?start_index={start_index}&items_per_page={items_per_page}', auth=(api_key, ''))
        
        #Add API status code to the dictionary
        status_codes[director_id] = {
            'status_code': response.status_code,
            'timestamp': str(datetime.now()),
            'request_number': request_number ,
        }
        
        request_number += 1
        if request_number > 1100:
            print("sleeping")
            time.sleep(300)
            request_number = 0
    
        data = response.json()
        total_results = data['total_results']
        
        if response.status_code == 200:
            
            #Create a dataframe from the highest level of the json
            all_appointments_data = data.copy()
            del all_appointments_data['items']
            all_appointments_data = pd.json_normalize(all_appointments_data)
            
            #Add director id
            all_appointments_data['director_id'] = director_id
            
            for items in data['items']:
                
                #Create another dataframe from main part of json 
                #& combine the two dataframes (dropping duplicated columns)
                items_normalised = pd.json_normalize(items)
                dup_columns = set(all_appointments_data.columns).intersection(items_normalised.columns)
                items_normalised.drop(columns=dup_columns, inplace=True)
                combined_data = pd.concat([items_normalised, all_appointments_data], axis=1)
                
                #Add the dataframe to the list
                dir_list.append(combined_data)
    
        start_index += items_per_page

#Create dataframes
status_codes = pd.DataFrame.from_dict(status_codes).T
corporate_elite_appointments = pd.concat(dir_list)

print(status_codes['status_code'].value_counts())

#Function moving main columns of dataframe to start
def move_main_columns(df, main_columns):
    positions = list(range(0, (len(main_columns))))
    columns_values = {col: df.pop(col) for col in main_columns}
    for col, pos in zip(main_columns, positions):
        df.insert(pos, col, columns_values[col])
    
    return df

#Apply function to companies info
main_columns = ['director_id', 'name', 'appointed_to.company_number',
                'appointed_to.company_name']
corporate_elite_appointments = move_main_columns(corporate_elite_appointments, main_columns)

#Export appointments data and search statuses as csvs
corporate_elite_appointments.to_csv(f'{DATA_DIR}/outputs/corporate_elite_appointments.csv', index=False)
status_codes.to_csv(f'{DATA_DIR}/outputs/corproate_elite_appointment_search_statuses.csv', index=True)