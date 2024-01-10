import pandas as pd
from getters import get_fame_data
from utils import PROJECT_DIR
from tqdm import tqdm

DATA_DIR = f'{PROJECT_DIR}/fame_data'

#Upload fame data as list of dataframes
fame_data = get_fame_data(DATA_DIR)

#First dataframe in list of dataframes
data = fame_data[0]

#Function adding leading zeros to company id numbers
def leading_zeros(dataframe_column):
    dataframe_column = dataframe_column.astype('str')
    dataframe_column = dataframe_column.str.pad(width=8, side='left', fillchar='0')
    dataframe_column = dataframe_column.str.replace('00000nan', '')
    
    return(dataframe_column)

#Apply function to dataframe
data.company_no = leading_zeros(data.company_no)

#Create separate dataframe to later collate company name & numbers
company_names_and_numbers = data[['company_name','company_no']]

#List of the other dataframes to merge with the first dataframe
dfs_to_merge = fame_data[1:]

#Loop through the remaining dfs and merge with the first
for df in tqdm(dfs_to_merge):

    #Apply function adding leading zeros to company id numbers
    df.company_no = leading_zeros(df.company_no)
    
    #Add company names and numbers to that dataframe & drop company name column
    company_name_and_number = df[['company_name','company_no']]
    company_names_and_numbers = pd.concat([company_names_and_numbers, company_name_and_number])
    df.drop(columns=['company_name'], inplace=True)
    
    #Merge with other dataframes
    data = pd.merge(data, df, how='outer', on='company_no')

#Drop duplicates from the company names and numbers dataframe
company_names_and_numbers.drop_duplicates(inplace=True)

#Convert non-values in boolean columns to 'False'
columns = list(data.columns.values)
boolean_columns = []

substrings = ['dup', 'top']
for substring in substrings:
    for column in columns: 
        if substring in column:
            boolean_columns.append(column)

for column in boolean_columns:
      data[column] = ( 
          data[column].astype('boolean')
          .fillna(False)
          )

#Create new columns with years each company is in top 250 & 500 by turnover

#New empty columns populate in the loop
data['top_500_years_list'] = ''
data['top_250_years_list'] = ''
data['dup_years_list'] = ''

#Create list from dataframe index
index_loc = data.index.values.tolist()

for no in tqdm(index_loc):
    
    #Series of company info from dataframe row
    company_info = data.iloc[no]
    
    top_500_columns = company_info.filter(like='top_500', axis=0)
    filt = (top_500_columns == True)
    
    top_500_columns = (
        top_500_columns[filt]
        .to_frame()
        .reset_index()
        )
    
    top_500_columns.replace('top_500_','', regex=True, inplace=True) 
    
    #List of years the company is in top 500
    top_500_years = top_500_columns['index'].tolist()
    
    #Add to dataframe
    data.at[no,'top_500_years_list'] = top_500_years
    
    #Do the same for the top 250 and the duplicate companies columns
    
    top_250_columns = company_info.filter(like='top_250', axis=0)
    filt = (top_250_columns == True)
    
    top_250_columns = (
        top_250_columns[filt]
        .to_frame()
        .reset_index()
        )
    
    top_250_columns.replace('top_250_','', regex=True, inplace=True) 
    top_250_years = top_250_columns['index'].tolist()
    data.at[no,'top_250_years_list'] = top_250_years
    
    dup_columns = company_info.filter(like='dup', axis=0)
    filt = (dup_columns == True)
    
    dup_columns =  ( 
        dup_columns[filt]
        .to_frame()
        .reset_index()
        )
    
    dup_columns.replace('dup_','', regex=True, inplace=True)
    dup_years =  dup_columns['index'].tolist()
    data.at[no,'dup_years_list'] =  dup_years

#Add counts of the years in those new columns
data['top_250_years_count'] = data['top_250_years_list'].map(len)
data['top_500_years_count'] = data['top_500_years_list'].map(len)
data['dup_years_count'] = data['dup_years_list'].map(len)

#Drop companies in top 500 less than two years
data.drop(data[data['top_500_years_count'] < 2].index, inplace = True)

#Order dataframe columns alphabetically by columns name
data = data.reindex(sorted(data.columns), axis=1)

#Add back in any missing company names
named_companies = data[['company_name', 'company_no']].dropna()
names_to_add = ( 
    company_names_and_numbers[~company_names_and_numbers.company_no.isin(named_companies.company_no)]
    .rename(columns = {'company_name':'missing_names'})
    )
data = pd.merge(data, names_to_add, how='left', on='company_no')
data['company_name'] = data['company_name'].fillna(data['missing_names'])
data.drop(columns=['missing_names'], inplace=True)

#Export the dataframe as a csv
DATA_DIR = f'{PROJECT_DIR}'
data.to_csv(f'{DATA_DIR}/outputs/companies.csv', index=False)