#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 11:55:07 2024

@author: narzaninmassoumi
"""

import requests
import pandas as pd
import time
from datetime import datetime
from tqdm import tqdm
from getters import get_corporate_elite
from utils import PROJECT_DIR

DATA_DIR = f'{PROJECT_DIR}/'

TEST = False

#Upload corporate elite data
corporate_elite = get_corporate_elite(DATA_DIR)

#Create list of director ids to populate API queries
director_ids = (
    corporate_elite['director_id'].drop_duplicates() 
    .tolist()
    )

if TEST: director_ids = director_ids[0:10]

#Upload API key from main folder
api_key = open(f'{DATA_DIR}/api_key.txt', 'r')
api_key = (api_key.read())

#URL for individuals' appointments
base_url = 'https://api.company-information.service.gov.uk/officers/'

#Empty list and dictionary to populate 
dir_list = []
status_codes = {}
failed_requests = []

request_number = 0

for id in tqdm(director_ids):
    items_per_page = 35
    start_index = 0
    total_results = float('inf')
    
    while start_index < total_results:
        response = requests.get(f'{base_url}{id}/appointments?start_index={start_index}&items_per_page={items_per_page}', auth=(api_key, ''))
        
        request_number += 1 
        
        if request_number > 1100:
            print("sleeping")
            time.sleep(300)
            request_number = 0
        
        #Add API status code to the dictionary
        status_codes[id] = {
            'status_code': response.status_code,
            'timestamp': str(datetime.now()),
            'request_number': request_number ,
        }
        
        #If request was success extract data
        if response.status_code == 200:
            data = response.json()
            total_results = data['total_results']
            
            #Create a dataframe from the highest level of the json
            all_appointments_data = data.copy()
            del all_appointments_data['items']
            all_appointments_data = pd.json_normalize(all_appointments_data)
            
            #Add director id
            all_appointments_data['director_id'] = id
            
            #Create another dataframe from the appointments data ('items')
            for items in data['items']:
                items_normalised = pd.json_normalize(items)
                
                #Drop columns alread in the data
                dup_columns = set(all_appointments_data.columns).intersection(items_normalised.columns)
                items_normalised.drop(columns=dup_columns, inplace=True)
                
                #Combine with main data
                combined_data = pd.concat([items_normalised, all_appointments_data], axis=1)
                
                #Add the combined dataframe to the results list
                dir_list.append(combined_data)
        else:
            #If request was not successful set total results to 0 to break the while loop
            total_results = 0
            
            #And add to failed requests list
            failed_requests.append(id)
    
        start_index += items_per_page

#Second loop to retry failed requests

print('Running second search on failed requests...')

for id in tqdm(failed_requests):
    items_per_page = 35
    start_index = 0
    total_results = float('inf')
    
    while start_index < total_results:
        response = requests.get(f'{base_url}{id}/?start_index={start_index}&items_per_page={items_per_page}', auth=(api_key, ''))
        
        request_number += 1 
        
        if request_number > 1100:
            print("sleeping")
            time.sleep(300)
            request_number = 0
        
        #Add API status code to the dictionary
        status_codes[id] = {
            'status_code': response.status_code,
            'timestamp': str(datetime.now()),
            'request_number': request_number ,
        }
        
        #If request was success extract data
        if response.status_code == 200:
            data = response.json()
            total_results = data['total_results']
            
            #Create a dataframe from the highest level of the json
            all_appointments_data = data.copy()
            del all_appointments_data['items']
            all_appointments_data = pd.json_normalize(all_appointments_data)
            
            #Add director id
            all_appointments_data['director_id'] = id
            
            #Create another dataframe from the appointments data ('items')
            for items in data['items']:
                items_normalised = pd.json_normalize(items)
                
                #Drop columns alread in the data
                dup_columns = set(all_appointments_data.columns).intersection(items_normalised.columns)
                items_normalised.drop(columns=dup_columns, inplace=True)
                
                #Combine with main data
                combined_data = pd.concat([items_normalised, all_appointments_data], axis=1)
                
                #Add the combined dataframe to the results list
                dir_list.append(combined_data)
        else:
            #If request was not successful set total results to 0 to break the while loop
            total_results = 0
    
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
status_codes.to_csv(f'{DATA_DIR}/outputs/corporate_elite_appointment_search_statuses.csv', index=True)