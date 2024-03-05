import pandas as pd
from utils import PROJECT_DIR
from getters import get_charities, get_private_schools

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
private_schools = get_private_schools(DATA_DIR)
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
        .replace('nan', None)
        )
    
    return df[column]

#Apply to both dataframes
charities['stipped_url'] = strip_url(charities, 'charity_contact_web')
private_schools['stripped_url'] = strip_url(private_schools, 'SchoolWebsite')

#Create lists of stripped urls from private schools dataframe
private_school_urls = (
                       private_schools['stripped_url'].dropna()
                       .drop_duplicates()
                       .tolist()
                       )

#Check if the charity urls are in the list
charities['private_school'] = charities['stipped_url'].isin(private_school_urls)

#Produce Series of organisation numbers based on match
filt = charities['private_school'] == True
private_school_ids = charities['organisation_number'].loc[filt]

#Produce list of post codes of private schools
private_school_postcodes = (
    private_schools['Postcode'].str.strip()
    .dropna()
    .drop_duplicates()
    .tolist()
    )

#Identify matching postcodes in charity data
charities['post_code_match'] = charities['charity_contact_postcode'].isin(private_school_postcodes)
post_code_matches = charities.loc[(charities['post_code_match'])]

#Function to check if 'school' substring is in charity name, address or activies
def contains_school(row):
    for col in ['charity_name', 'charity_contact_address1', 'charity_activities']:
        if 'school' in str(row[col]).lower():
            return True
    return False

#Apply the function and filter dataframe
post_code_matches['contains_school_substring'] = post_code_matches.apply(contains_school, axis=1)
post_code_matches = post_code_matches.loc[(post_code_matches['contains_school_substring'])]

#Combine with id list from website matches
private_school_ids = pd.concat([private_school_ids, post_code_matches['organisation_number']]).drop_duplicates()

#Export ids as text file
private_school_ids.to_csv(f'{DATA_DIR}/other_data_sources/private_school_charities.txt', header=True, index=False)