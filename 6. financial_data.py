import pandas as pd
import numpy as np
from utils import PROJECT_DIR
from getters import get_CRCs, get_annual_return_parta

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
CRCs = get_CRCs(DATA_DIR)
parta = get_annual_return_parta(DATA_DIR)

#Run convert_dtypes to better infer the datatypes in the annual returns data
parta = parta.convert_dtypes()

#Combine income from government grants and contracts
parta['government_income'] = parta['income_from_government_contracts'] + parta['income_from_government_grants']        

#Mean of three numeric variables
means = ( 
    parta.groupby('organisation_number', as_index=False)[['total_gross_income', 'government_income', 'count_volunteers']].mean()
    .rename(columns = {'total_gross_income':'mean_gross_income', 
                       'government_income':'mean_government_income', 'count_volunteers': 'mean_count_volunteers'})
    )

#Assign income levels to 'bins' using UK Civil Society Almanac categories#
bins = [-np.inf,10000, 100000, 1000000, 10000000, 100000000, np.inf]
bin_labels = ['Micro', 'Small', 'Medium', 'Large', 'Major', 'Super-major']
means['median_income_ord'] = pd.cut(means['mean_gross_income'], bins, labels=bin_labels, include_lowest =True)

#List of the boolean columns in the annual returns dataframe
boolean_columns = parta.select_dtypes(include='bool').columns

#Group boolean columns by organisation number, returning True if True in any year and otherwise either False or no value
booleans_df = parta[['organisation_number'] + list(boolean_columns)]
grouped_booleans = booleans_df.groupby('organisation_number', as_index=False).mean()
grouped_booleans = ( 
                grouped_booleans.drop(['organisation_number'], axis=1)
                .applymap(lambda x: pd.NA if pd.isna(x) else False if x == 0 else True if x > 0 else pd.NA)
                )
booleans_df = pd.concat([means,grouped_booleans], axis=1)

#Merge the data
CRCs = pd.merge(CRCs, booleans_df, how='left', on='organisation_number')

#Export data
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)