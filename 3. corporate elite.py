import pandas as pd
from tqdm import tqdm
from utils import PROJECT_DIR
from getters import get_companies, get_appointments
import ast

DATA_DIR = f'{PROJECT_DIR}/'

#Upload company data
companies = get_companies(DATA_DIR)

#Uploads appointments data
appointments_data = get_appointments(DATA_DIR)

#Replace missing time/dates with valuesoutside of time period
appointments_data['appointed_on'].fillna('1990-01-01', inplace=True)
appointments_data['resigned_on'].fillna('2024-01-01', inplace=True)

#Convert appointment/resignation dates into pandas date time
appointments_data['appointed_on'] = pd.to_datetime(appointments_data['appointed_on'])
appointments_data['resigned_on'] = pd.to_datetime(appointments_data['resigned_on'])

#Filter for company directors (dropping secretaries & other roles)
filt = (appointments_data.officer_role == 'director')
directors = appointments_data[filt]
directors.reset_index(drop=True, inplace=True)

#Create new variable for British honourifics
directors['honourifics'] = directors['name'].str.contains(' Lord| Lady| Sir| Dame| Baron| Baroness| Lord |Lady |Sir |Dame |Baron |Baroness |Earl of', case=False)

#Create company numbers from the appointments data
directors['company_no'] = directors['links.self'].str[9:17]

#Create director ids from the appointments data
directors['director_id'] = ( 
    directors['links.officer.appointments'].str.replace('/officers/', '', regex=False)
        .str.replace('/appointments', '', regex=False)
        )

#Join the years the a company is in top 250 & 500 to appointments data as lists
directors = pd.merge(directors, companies[['company_no', 'top_250_years_list', 'top_500_years_list']], how='left', on='company_no')
directors['top_500_years_list'] = directors['top_500_years_list'].apply(ast.literal_eval)
directors['top_250_years_list'] = directors['top_250_years_list'].apply(ast.literal_eval)

'''Loop checking if appointment period is in top 250 & 500 years of company'''

#List from dataframe index
index_loc = directors.index.values.tolist()

#Create empty columns to populate with true if appointment overlaps
directors['top_500_appointment'] = False
directors['top_250_appointment'] = False

#Loop checking if appointment was in any year company was in either top lists
for row in tqdm(index_loc):
    appointed_on = directors.at[row, 'appointed_on']       
    resigned_on = directors.at[row, 'resigned_on']
    years_list = pd.to_datetime(directors.at[row, 'top_500_years_list'])
    for year in years_list:
        yearend = year + pd.DateOffset(days=364)
        if appointed_on < yearend and resigned_on >= year:
            directors.at[row, 'top_500_appointment'] = True
        years_list = pd.to_datetime(directors.at[row, 'top_250_years_list'])
        for year in years_list:
            yearend = year + pd.DateOffset(days=364)
            if appointed_on < yearend and resigned_on >= year:
                directors.at[row, 'top_250_appointment'] = True

#Filter for top 500 company directorships
filt = (directors.top_500_appointment == True)
top_500_dir_info = directors[filt]

#Export the data as a csv
top_500_dir_info.to_csv(f'{DATA_DIR}/outputs/corporate_elite.csv', index=False)