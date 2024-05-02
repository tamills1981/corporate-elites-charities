import pandas as pd
import numpy as np
from utils import PROJECT_DIR
from getters import get_CRCs, get_annual_return_parta, get_annual_return_history

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
CRCs = get_CRCs(DATA_DIR)
parta = get_annual_return_parta(DATA_DIR)
ar_history = get_annual_return_history(DATA_DIR)

#Run convert_dtypes to better infer the datatypes in the annual returns data
parta = parta.convert_dtypes()

#Combine income from government grants and contracts
parta['government_income'] = parta['income_from_government_contracts'] + parta['income_from_government_grants']        

#Mean of two numeric variables
means = ( 
    parta.groupby('organisation_number', as_index=False)[['government_income', 'count_volunteers']].mean()
    .rename(columns = {'government_income':'mean_government_income', 'count_volunteers': 'mean_count_volunteers'})
    )

#Extract mean income data from annual return history and parta files
ar_history_income_data = ar_history[['organisation_number', 'ar_cycle_reference', 'total_gross_income']].dropna()
parta_income_data = parta[['organisation_number', 'ar_cycle_reference', 'total_gross_income']].dropna()
income_data = (
    pd.concat([ar_history_income_data, parta_income_data])
    .drop_duplicates(keep='first')
    .reset_index(drop=True)
    )

mean_income = (
               income_data.groupby('organisation_number')['total_gross_income'].mean()
               .reset_index()
               .rename(columns = {'total_gross_income':'mean_gross_income'})
               )

#Assign income levels to 'bins' using the NCVO's UK Civil Society Almanac categories
bins = [-np.inf,10000, 100000, 1000000, 10000000, 100000000, np.inf]
bin_labels = ['Micro', 'Small', 'Medium', 'Large', 'Major', 'Super-major']
mean_income['ncvo_size_categories'] = pd.cut(mean_income['mean_gross_income'], bins, labels=bin_labels, include_lowest =True)

#List of the boolean columns in the annual returns dataframe
boolean_columns = parta.select_dtypes(include='bool').columns

#Group boolean columns by organisation number, returning True if True in any year and otherwise either False or no value
booleans_df = parta[['organisation_number'] + list(boolean_columns)]
grouped_booleans = booleans_df.groupby('organisation_number', as_index=False).mean()
grouped_booleans = ( 
                grouped_booleans.drop(['organisation_number'], axis=1)
                .applymap(lambda x: pd.NA if pd.isna(x) else False if x == 0 else True if x > 0 else pd.NA)
                )
parta_data = pd.concat([means, grouped_booleans], axis=1)

#Merge the data
CRCs = pd.merge(CRCs, parta_data, how='left', on='organisation_number')
CRCs = pd.merge(CRCs, mean_income, how='left', on='organisation_number')

#Assign income level to three quantiles
CRCs['charity_size'] = pd.qcut(CRCs['mean_gross_income'], q=3, labels=['small', 'medium', 'large'])

#Export data
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)