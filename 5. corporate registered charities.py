import pandas as pd
from utils import PROJECT_DIR
from getters import get_charities, get_royal_patronage

DATA_DIR = f'{PROJECT_DIR}/'

#Upload charity data
charity_data = get_charities(DATA_DIR)
royal_patronage = get_royal_patronage(DATA_DIR)

#Drop charities registered after 2023 
filt = (charity_data['date_of_registration'] < '2023')
charity_data = charity_data.loc[filt]

#Drop charities without a company registration number
unregistered_charities = charity_data['charity_company_registration_number'].isnull() 
CRCs = (
        charity_data.loc[~unregistered_charities]
        .reset_index(drop=True)
        )

#Drop some unwanted columns
CRCs = CRCs[['organisation_number', 'registered_charity_number', 'charity_company_registration_number',
             'charity_name', 'charity_activities', 'date_of_registration', 
             'charity_contact_address1', 'charity_contact_address2', 'charity_contact_address3', 
             'charity_contact_address4', 'charity_contact_address5', 'charity_contact_postcode', 
             'charity_contact_phone', 'charity_contact_email', 'charity_contact_web', 'charity_gift_aid', 
             'charity_has_land']]

#Create new royal patronage boolean variable 
CRCs['royal_patronage'] = CRCs['organisation_number'].isin(royal_patronage['registered_charity_number']) | CRCs['registered_charity_number'].isin(royal_patronage['registered_charity_number'])

#Create new charity age variable
enddate = pd.to_datetime('31-12-2022')
def calculate_age(date_of_reg):
    return enddate.year - date_of_reg.year
CRCs['charity_age'] = CRCs['date_of_registration'].apply(calculate_age)

#Export the data as a csv
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)