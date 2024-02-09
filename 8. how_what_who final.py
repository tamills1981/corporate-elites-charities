import pandas as pd
from utils import PROJECT_DIR
from getters import get_CRCs, get_charity_classification_data

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
CRCs = get_CRCs(DATA_DIR)
classification = get_charity_classification_data(DATA_DIR)

#Combine type & description into single column
classification['CRC_activities'] = classification['classification_type'] + '_' + classification['classification_description'] 

#Drop some unwanted columns
classification.drop(columns=['City date_of_extract', 'registered_charity_number',
        'linked_charity_number', 'classification_code', 'classification_type',
        'classification_description',], inplace=True)

#Pivot dataframe so the presence of categories are boolean columns
classification['boolean'] = True
classification_pivot = classification.pivot(index='organisation_number', columns='CRC_activities', values='boolean').reset_index().fillna(False)

#Convert org number to int
classification_pivot['organisation_number'] = classification_pivot['organisation_number'].astype(int)

#Join data
CRCs = pd.merge(CRCs, classification_pivot, how='left', on='organisation_number')

#Convert the column names to lowercase and replace spaces with underscores
CRCs.columns = [col.lower().replace(' ', '_') for col in CRCs.columns]

#Export CSV
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)