#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 23 13:01:40 2024

@author: narzaninmassoumi
"""

import pandas as pd
from utils import PROJECT_DIR
from getters import get_charities, get_royal_patronage, get_corporate_elite_appointments, get_private_school_charities, get_oxbridge_charities, get_think_tank_charities, get_high_culture

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
charity_data = get_charities(DATA_DIR)
royal_patronage = get_royal_patronage(DATA_DIR)
corporate_elite_appointments = get_corporate_elite_appointments(DATA_DIR)
private_school_charities = get_private_school_charities(DATA_DIR)
oxbridge_charities = get_oxbridge_charities(DATA_DIR)
think_tank_charities = get_think_tank_charities(DATA_DIR)
high_culture_charities = get_high_culture(DATA_DIR)

#Period start and end dates
start_date = pd.to_datetime('2013-01-01')
end_date = pd.to_datetime('2022-12-31')

#Drop charities registered after 2023 
filt = (charity_data['date_of_registration'] <= end_date)
charity_data = charity_data.loc[filt]

#Create charity age variable
def calculate_age(date_of_reg):
    return end_date.year - date_of_reg.year
charity_data['charity_age'] = charity_data['date_of_registration'].apply(calculate_age)

#Years in which charity was de-registered during time period
charity_data['years_deregistered'] = (end_date - charity_data['date_of_removal']) // pd.Timedelta(days=365) + 1
charity_data['years_deregistered'] = charity_data['years_deregistered'].fillna(0)

#Years charity was not registered (now including if established during time period)
def create_years_not_active(charity_age, years_deregistered):
    if charity_age < 10:
        years = 10 - charity_age
        years_not_active = years_deregistered + years
        return years_not_active
    else:
        return years_deregistered
charity_data['years_not_active'] = charity_data.apply(lambda x: create_years_not_active(x['charity_age'], x['years_deregistered']), axis=1)

#Drop charities without a company registration number
unregistered_charities = charity_data['charity_company_registration_number'].isnull() 
CRCs = (
        charity_data.loc[~unregistered_charities]
        .reset_index(drop=True)
        )

#Drop some unwanted columns
CRCs = CRCs[['organisation_number', 'registered_charity_number', 'charity_company_registration_number',
              'charity_name', 'charity_activities', 'date_of_registration', 'charity_age', 'years_not_active', 'charity_contact_address1', 'charity_contact_address2', 'charity_contact_address3', 
              'charity_contact_address4', 'charity_contact_address5', 'charity_contact_postcode', 
              'charity_contact_phone', 'charity_contact_email', 'charity_contact_web', 'charity_gift_aid', 
              'charity_has_land']]

#Create ordinal variable for charity age with three equal 'bins'
bin_labels = ['0-20', '21-40', '41+']
CRCs['charity_age_ord'] = pd.cut(CRCs['charity_age'], bins=3, labels = bin_labels)

#Create royal patronage boolean variable 
CRCs['royal_patronage'] = CRCs['organisation_number'].isin(royal_patronage['registered_charity_number'])

#Create private school charity variable
CRCs['private_school'] = CRCs['organisation_number'].isin(private_school_charities['organisation_number'])

#Create Oxbridge charity variable
CRCs['oxbridge'] = CRCs['organisation_number'].isin(oxbridge_charities['organisation_number'])

#Create 'high culture' charity variable
CRCs['high_culture'] = CRCs['organisation_number'].isin(high_culture_charities['organisation_number'])

#Create think tank variable 
CRCs['think_tank'] = CRCs['organisation_number'].isin(think_tank_charities['organisation_number'])

#Create corproate elite trustee variable


# Convert to datetime
corporate_elite_appointments['resigned_on'] = pd.to_datetime(corporate_elite_appointments['resigned_on'])
corporate_elite_appointments['appointed_on'] = pd.to_datetime(corporate_elite_appointments['appointed_on'])

# Filter to include only appointments active during the period (i.e., appointments made before end date and resignations after start date or not resigned)
filt_active_during_period = (
    (corporate_elite_appointments['appointed_on'] <= end_date) & 
    (corporate_elite_appointments['resigned_on'].isna() | (corporate_elite_appointments['resigned_on'] > start_date))
)

corporate_elite_appointments = corporate_elite_appointments.loc[filt_active_during_period]

# Create corporate elite trustee variable
CRCs['corporate_elite_trustee'] = CRCs['charity_company_registration_number'].isin(corporate_elite_appointments['appointed_to.company_number'])

#Export the data as a csv
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)