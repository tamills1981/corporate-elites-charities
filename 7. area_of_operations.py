#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb  9 10:48:35 2024

@author: narzaninmassoumi
"""

import pandas as pd
import numpy as np

from utils import PROJECT_DIR
from getters import get_CRCs, get_area_of_operations_data, get_authorities_regions_data

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
CRCs = get_CRCs(DATA_DIR)
area_of_operations = get_area_of_operations_data(DATA_DIR)

#Run convert_dtypes to better infer the datatypes
area_of_operations = area_of_operations.convert_dtypes()

#Trim df to only relevant columns
area_of_operations = area_of_operations[['organisation_number', 'geographic_area_type',
                                          'geographic_area_description', 'parent_geographic_area_type',
                                          'parent_geographic_area_description', 'welsh_ind']]

#Drop columns not in CRC dataframe
area_of_operations['organisation_number'] = area_of_operations['organisation_number'].astype(int)
area_of_operations = area_of_operations[area_of_operations['organisation_number'].isin(CRCs['organisation_number'])]

#Load UK region data and merge it with the area_of_operations dataframe
authorities_regions = get_authorities_regions_data(DATA_DIR)
area_of_operations = pd.merge(area_of_operations, authorities_regions, how='left', on='geographic_area_description')

#Create a dataframe with just the international operations
filt = (area_of_operations['parent_geographic_area_type'] == 'Continent')
int_ops = area_of_operations.loc[filt]

#Group by organisation number, gathering the different continents into a set
int_ops = (
        int_ops.groupby('organisation_number')['parent_geographic_area_description'].apply(set).apply(sorted)
        .to_frame()
        .reset_index()
        )

#Create variable for length of that set & add 'Global' to any containing two or more continents
int_ops['length'] = int_ops['parent_geographic_area_description'].str.len()
int_ops['area_of_operation'] = int_ops['length'].apply(lambda x: np.nan if x == 1 else 'Global')

#Create new column with set items as a string
int_ops['continents_of_op'] = int_ops['parent_geographic_area_description'].apply(', '.join)

#Fill the empty rows in the 'area of operation' column with the single continents
int_ops['area_of_operation'] = int_ops['area_of_operation'].fillna(int_ops['continents_of_op'])

#Tweak the df
int_ops = (
        int_ops[['organisation_number', 'area_of_operation', 'continents_of_op', 'length']]
        .rename(columns = {'length':'continents_of_op_count'})
        )

#Produce a column for countries of operation (using variation of above code)
country_ops = area_of_operations.loc[filt].groupby('organisation_number')['geographic_area_description'].agg(lambda x: sorted(set(x))).reset_index()
country_ops['countries_of_op_count'] = country_ops['geographic_area_description'].str.len()
country_ops['countries_of_op'] = country_ops['geographic_area_description'].apply(', '.join)
country_ops.drop(columns='geographic_area_description', inplace=True)


#Filter df for Northern Ireland & Scotland operations
scot_NI = country_ops[country_ops['countries_of_op'].isin(['Northern Ireland, Scotland', 'Northern Ireland', 'Scotland'])].copy()
scot_NI['area_of_operation'] = 'UK'
scot_NI.drop(columns=['countries_of_op_count'], inplace=True)

#Drop NI & Scotland from other dfs (since these are not international)
int_ops = int_ops[~int_ops.organisation_number.isin(scot_NI.organisation_number)]
country_ops = country_ops[~country_ops.organisation_number.isin(scot_NI.organisation_number)]

#Combine the continent & countries dfs into single df
int_ops = pd.merge(int_ops, country_ops, how='outer', on='organisation_number')

del country_ops

# This section restructures the charities active in England and Wales

# Drop global & NI/Scotland charities from the DataFrame
excluded_orgs = set(int_ops['organisation_number']).union(scot_NI['organisation_number'])
national_ops = area_of_operations[~area_of_operations['organisation_number'].isin(excluded_orgs)]

# Split off data into the regional and local authority entries
regional = national_ops[national_ops['geographic_area_type'] == 'Region']
local = national_ops[national_ops['geographic_area_type'] == 'Local Authority']

# Process regional data
regional = regional.drop(columns=['geographic_area_type', 'parent_geographic_area_type',
                                  'parent_geographic_area_description', 'local_authority_region'])
regional = regional.rename(columns={'geographic_area_description': 'area_of_operation'})
regional['area_of_operation'] = regional['area_of_operation'].replace(
    ['Throughout England And Wales', 'Throughout England', 'Throughout Wales', 'Throughout London'],
    ['England & Wales', 'England', 'Wales', 'Greater London'], regex=True)

# Drop regional operations from the local DataFrame and keep relevant columns
local = local[~local['organisation_number'].isin(regional['organisation_number'])]
local = local[['organisation_number', 'geographic_area_description', 'local_authority_region']]

# Group by organisation number, gathering the different regions into a set and sort
df1 = local.groupby('organisation_number')['local_authority_region'].apply(lambda x: ', '.join(sorted(set(x)))).reset_index()
df1['length'] = df1['local_authority_region'].apply(lambda x: len(x.split(', ')))
df1['area_of_operation'] = df1['length'].apply(lambda x: 'England' if x > 1 else np.nan)
df1.rename(columns={'local_authority_region': 'regions_of_op'}, inplace=True)


# Create separate df of charities active in Wales
wales = df1[df1['regions_of_op'].str.contains('Wales') & (df1['length'] > 1)]
wales['area_of_operation'] = 'England & Wales'

# Drop Welsh operations from df1 and then join wales to the dataframe
df1 = pd.concat([wales, df1[~df1['organisation_number'].isin(wales['organisation_number'])]])

# Fill the empty rows in the 'area of operation' column with the single regions
df1['area_of_operation'].fillna(df1['regions_of_op'], inplace=True)

# Tweak the df
df1.drop(columns=['regions_of_op', 'length'], inplace=True)
df1.rename(columns={'regions_of_op_count': 'regions_of_op_count'}, inplace=True)

# Produce a column for local authorities of operation (using variation of above code)
df2 = local.groupby('organisation_number')['geographic_area_description'].apply(lambda x: ', '.join(sorted(set(x)))).reset_index()
df2.rename(columns={'geographic_area_description': 'LAs', 'Length': 'LAs_count'}, inplace=True)

# Combine the region & LA dfs into a single df
local_ops = pd.merge(df1, df2, how='outer', on='organisation_number')

# Combine the global, Scottish/Northern Ireland, Regional & Local into a single df. Join with original data and export
all_data = pd.concat([int_ops, scot_NI, regional, local_ops])


'''Combine the global, Scottish/Northern Ireland, Regional & Local into single 
df. Join with original data and export'''
all_data = pd.concat([int_ops, scot_NI, regional, local_ops])

#categorise regions

def categorise_region(region):
    global_categories = ['Global']
    international_categories = ['Africa', 'Asia', 'Europe', 'North America', 'South America', 'Oceania', 'Antarctica']
    regional_categories = ['England', 'Wales', 'Scotland', 'Northern Ireland', 'UK']
    # Assuming other regions are local

    if region in global_categories:
        return 'global'
    elif region in international_categories:
        return 'international'
    elif region in regional_categories:
        return 'regional'
    else:
        return 'local'
all_data['region_of_op'] = all_data['continents_of_op'].apply(categorise_region)

all_data = all_data[['region_of_op', 'area_of_operation', 'organisation_number']]

#Merge with the rest of the CRC data
CRCs = pd.merge(CRCs, all_data, how='left', on='organisation_number')

# #Export CSV
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)