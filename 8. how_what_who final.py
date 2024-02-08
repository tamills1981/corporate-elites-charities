# -*- coding: utf-8 -*-
"""
This script produces summary data on the charities' activities/beneficiaries
from the charity commission's charity classification data. It returns a True
if that category was recorded in any reporting year.

"""
import pandas as pd

from utils import PROJECT_DIR
from getters import get_charities
from getters import get_charity_classification_data

DATA_DIR = f'{PROJECT_DIR}/'


#Upload data

charity_data = get_charities(DATA_DIR)

classification = get_charity_classification_data(DATA_DIR)

# Rename the 'City date_of_extract' column to 'date_of_extract'
classification.rename(columns={'City date_of_extract': 'date_of_extract'}, inplace=True)

    
#Combine type & description into new variable & cut down df
classification['CRC_activities'] = classification['classification_type'] + ': ' + classification['classification_description'] 
classification.drop(columns=['date_of_extract', 'registered_charity_number',
       'linked_charity_number', 'classification_code', 'classification_type',
       'classification_description',], inplace=True)

#Run convert_dtypes to better infer the diffeerent datatypes
classification = classification.convert_dtypes()
classification['boolean'] = True
classification_pivot = classification.pivot(index='organisation_number', columns='CRC_activities', values='boolean')
classification_pivot.reset_index(inplace=True)

#Replace non-values with False
classification_pivot.fillna(False,inplace=True)

#Convert Boolean
classification_pivot = classification_pivot.convert_dtypes()

#Join data
CRCs = pd.merge(charity_data, classification_pivot, how='left', on='organisation_number')

#Export CSV
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)

