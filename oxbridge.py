import pandas as pd
from utils import PROJECT_DIR
from getters import get_charities

DATA_DIR = f'{PROJECT_DIR}/'

#Upload charities data
charities = get_charities(DATA_DIR)

#Function to strip URLs for matching
def strip_url(df, column):
    df[column] = (
        df[column].astype(str)
        .str.replace('http://', '')
        .str.replace('https://', '')
        .str.replace('www.', '')
        .apply(lambda x: x[:-1] if x.endswith('/') else x)
        .str.lower()
        .str.strip()
        )
    
    return df[column]

#Apply to dataframe
charities['stipped_url'] = strip_url(charities, 'charity_contact_web')

#Check if charity has oxbridge url
charities['oxbridge'] = charities['stipped_url'].str.contains('.ox.ac.uk|.cam.ac.uk')



#Export Oxbridge organisation numbers
filt = charities['oxbridge'] == True
oxbridge_ids = charities.loc[filt].drop_duplicates()
oxbridge_ids = oxbridge_ids['organisation_number']
oxbridge_ids.to_csv(f'{DATA_DIR}/other_data_sources/oxbridge.txt', header=True, index=False)
