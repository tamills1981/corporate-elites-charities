import pandas as pd
from utils import PROJECT_DIR
from getters import get_charities, get_think_tanks

DATA_DIR = f'{PROJECT_DIR}/'

# upload data
DATA_DIR = f'{PROJECT_DIR}/'
charity_data = get_charities(DATA_DIR)
think_tanks_df =get_think_tanks(DATA_DIR)

# Filter charities registered before 2023
filt = charity_data['date_of_registration'] < '2023'
df_charities = charity_data.loc[filt]

# clean urls

def clean_url(column):
    """Cleans URLs by stripping protocol, www, trailing slash, and setting to lowercase."""
    return (column.astype(str)
            .str.replace('http://', '', regex=False)
            .str.replace('https://', '', regex=False)
            .str.replace('www.', '', regex=False)
            .apply(lambda x: x[:-1] if x.endswith('/') else x)
            .str.lower()
            .str.strip()
            .replace('nan', pd.NA))

# Clean URLs in both dataframes
charity_data['cleaned_url'] = clean_url(charity_data['charity_contact_web'])
think_tanks_df['cleaned_url'] = clean_url(think_tanks_df['website'])

# Ensure uniqueness 
think_tanks_urls = set(think_tanks_df['cleaned_url'].dropna().unique())

# Check if the charity URLs match with the think tanks URLs
charity_data['is_think_tank'] = charity_data['cleaned_url'].apply(lambda x: x in think_tanks_urls)

# Filter charities that are identified as think tanks
charities_think_tank = charity_data[charity_data['is_think_tank']]

#Export ids as text file
charities_think_tank.to_csv(f'{DATA_DIR}/other_data_sources/think_tank_charities.csv', header=True, index=False)
