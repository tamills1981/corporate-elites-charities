import requests
import pandas as pd
from tqdm import tqdm
from fuzzywuzzy import fuzz

from utils import PROJECT_DIR
from getters import CRCs_no_co_no

DATA_DIR = f'{PROJECT_DIR}/'

TEST = False

#Upload charities data
charities = CRCs_no_co_no(DATA_DIR)

#Create list from charity names
charities = charities.drop_duplicates(subset='charity_name', keep='first')
charity_names = charities['charity_name'].to_list()

if TEST: charity_names = charity_names[:10]

#Upload API key from main folder
api_key = open(f"{DATA_DIR}/api_key.txt", "r")
api_key = (api_key.read())

base_url = 'https://api.company-information.service.gov.uk/search/companies'

results_list = []

for name in tqdm(charity_names):
    response = requests.get(f"{base_url}?q={name}", auth=(api_key, ''))
    result = response.json()
    results_dict = {
        'charity_name':name,
        'best_match': result['items'][0]['title'],
        'company_number': result['items'][0]['company_number']
        }
    results_list.append(results_dict)

results_df = pd.DataFrame.from_dict(results_list)

#Function to compare searched name with returned names
def compare_names(row):
    name1_lower = str(row['charity_name']).lower().replace('limited', 'ltd')
    name2_lower = str(row['best_match']).lower().replace('limited', 'ltd')
    return fuzz.token_sort_ratio(name1_lower, name2_lower)

#Apply the function to the df
results_df['fuzzy_match'] = results_df.apply(compare_names, axis=1)

#Add column for manually confirming match
results_df['confirm_company_no'] = (results_df['fuzzy_match'] == 100)

#Add column for any notes
results_df['notes'] = ''

#Export as CSV
results_df.to_csv(f'{DATA_DIR}/outputs/confirm_charity_company_no.csv', index=True)