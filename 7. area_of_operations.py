#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 14:11:10 2024

@author: narzaninmassoumi
"""

# Import necessary libraries
import pandas as pd
import numpy as np
import os
import pickle
import json
import codecs


from utils import PROJECT_DIR
from getters import get_charities
from getters import get_area_of_operations_data
from getters import get_authorities_regions_data

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
charity_data = get_charities(DATA_DIR)
area_of_operations = get_area_of_operations_data(DATA_DIR)

# Convert data types for better inference
area_of_operations = area_of_operations.convert_dtypes()

# # Select only relevant columns from the area_of_operations DataFrame
area_of_operations = area_of_operations[['organisation_number', 'geographic_area_type',
                                          'geographic_area_description', 'parent_geographic_area_type',
                                          'parent_geographic_area_description', 'welsh_ind']]

# # Load UK region data and merge it with the area_of_operations DataFrame
    
authorities_regions =get_authorities_regions_data(DATA_DIR)

area_of_operations = pd.merge(area_of_operations, authorities_regions, how='left', on='geographic_area_description')

'''In this section we restructure the international operations data'''

#Create a dataframe with just the international operations
filt1 = (area_of_operations['parent_geographic_area_type'] == 'Continent')
global_ops = area_of_operations.loc[filt1]

#Group by organisation number, gathering the different continents into a set
df1 = global_ops.groupby('organisation_number')['parent_geographic_area_description'].apply(set)

#Convert the resulting series to a df and reset index
df1 = df1.to_frame()
df1.reset_index(inplace=True)

#Create variable for length of that set & add 'Global' to any containing two or more continents
df1['Length'] = df1['parent_geographic_area_description'].str.len()
df1['area_of_operation'] = df1['Length'].apply(lambda x: np.nan if x == 1 else 'Global')

#Sort the items from each set & convert to a string
df1['parent_geographic_area_description'] = df1['parent_geographic_area_description'].apply(sorted)
df1['continents_of_op'] = df1['parent_geographic_area_description'].apply(', '.join)

#Fill the empty rows in the 'area of operation' column with the single continents
df1['area_of_operation'] = df1['area_of_operation'].fillna(df1['continents_of_op'])

#Tweak the df
df1 = df1[['organisation_number', 'area_of_operation', 'continents_of_op', 'Length']]
df1.rename(columns = {'Length':'continents_of_op_count'}, inplace=True)

#Produce a column for countries of operation (using variation of above code)
df2 = global_ops.groupby('organisation_number')['geographic_area_description'].apply(set)
df2 = df2.to_frame()
df2.reset_index(inplace=True)
df2['Length'] = df2['geographic_area_description'].str.len()
df2['geographic_area_description'] = df2['geographic_area_description'].apply(sorted)
df2['geographic_area_description'] = df2['geographic_area_description'].apply(', '.join)
df2.rename(columns = {'Length':'countries_of_op_count', 'geographic_area_description':'countries_of_op'}, inplace=True)

#Filter for Northern Ireland & Scotland operations
filt2 = df2["countries_of_op"].isin(["Northern Ireland, Scotland", "Northern Ireland", "Scotland" ])
scot_NI = df2[filt2]
scot_NI['area_of_operation'] = 'UK'
scot_NI.drop(columns=['countries_of_op_count'], inplace=True)

#Drop NI & Scotland from other dfs (since these are not international)
df1 = df1[~df1.organisation_number.isin(scot_NI.organisation_number)]
df2 = df2[~df2.organisation_number.isin(scot_NI.organisation_number)]

#Combine the continent & countries dfs into single df
global_ops = pd.merge(df1, df2, how='outer', on='organisation_number')

del df1, df2

'''Next we do the charities active in England and Wales...'''

#Drop global & NI/Scotland charities from the df
national_ops = area_of_operations[~area_of_operations.organisation_number.isin(global_ops.organisation_number)]
national_ops = national_ops[~national_ops.organisation_number.isin(scot_NI.organisation_number)]

#Split off data into the regional and the local authority entries
filt3 = (national_ops['geographic_area_type'] == 'Region')
regional = national_ops.loc[filt3]

filt4 = (national_ops['geographic_area_type'] == 'Local Authority')
local = national_ops.loc[filt4]

del national_ops

#Trim & edit regional ops df
regional.drop_duplicates(inplace=True) #NB only one charity
regional.drop(columns=['geographic_area_type', 'parent_geographic_area_type',
       'parent_geographic_area_description', 'local_authority_region'], inplace=True)
regional['geographic_area_description'] = regional['geographic_area_description'].str.replace('Throughout England And Wales', 'England & Wales', regex=True)
regional['geographic_area_description'] = regional['geographic_area_description'].str.replace('Throughout England', 'England', regex=True)
regional['geographic_area_description'] = regional['geographic_area_description'].str.replace('Throughout Wales', 'Wales', regex=True)
regional['geographic_area_description'] = regional['geographic_area_description'].str.replace('Throughout London', 'Greater London', regex=True)
regional.rename(columns = {'geographic_area_description':'area_of_operation'}, inplace=True)

#Drop regional ops from local df
local = local[~local.organisation_number.isin(regional.organisation_number)]
local = local[['organisation_number', 'geographic_area_description', 'local_authority_region']]

#Group by organisation number, gathering the different continents into a set
df3 = local.groupby('organisation_number')['local_authority_region'].apply(set)

#Convert the resulting series to a df and reset index
df3 = df3.to_frame()
df3.reset_index(inplace=True)

#Create variable for length of that set & add 'England' to any containing two or more regions
df3['Length'] = df3['local_authority_region'].str.len()
df3['area_of_operation'] = df3['Length'].apply(lambda x: np.nan if x == 1 else 'England')

#Sort the items from each set & convert to a string
df3['local_authority_region'] = df3['local_authority_region'].apply(sorted)
df3['regions_of_op'] = df3['local_authority_region'].apply(', '.join)

#Create separate df of charities active in Wales
df3Wales = df3.query('Length > 1')
df3Wales['Wales'] = df3Wales['regions_of_op'].str.contains('Wales')
filt5 = (df3Wales['Wales'] == True)
df3Wales = df3Wales.loc[filt5]
df3Wales['area_of_operation'] = df3Wales['area_of_operation'] = 'England & Wales'
df3Wales.drop(columns=['Wales'], inplace=True)

#Drop Welsh operations from df3 and then join df3Wales to the dataframe
df3 = df3[~df3.organisation_number.isin(df3Wales.organisation_number)]
df3 = pd.concat([df3Wales,df3])
del df3Wales

#Fill the empty rows in the 'area of operation' column with the single regions
df3['area_of_operation'] = df3['area_of_operation'].fillna(df3['regions_of_op'])

#Tweak the df
df3.drop(columns=['local_authority_region'], inplace=True)
df3.rename(columns = {'Length':'regions_of_op_count'}, inplace=True)

#Produce a column for local authorities of operation (using variation of above code)
df4 = local.groupby('organisation_number')['geographic_area_description'].apply(set)
df4 = df4.to_frame()
df4.reset_index(inplace=True)
df4['Length'] = df4['geographic_area_description'].str.len() 
df4['LAs'] = df4['geographic_area_description'].apply(sorted)
df4['LAs'] = df4['LAs'].apply(', '.join)
df4.rename(columns = {'Length':'LAs_count'}, inplace=True)
df4.drop(columns=['geographic_area_description'], inplace=True)

del local

#Combine the region & LA dfs into single df
local_ops = pd.merge(df3, df4, how='outer', on='organisation_number')
del df3, df4

'''Combine the global, Scottish/Northern Ireland, Regional & Local into single 
df. Join with original data and export'''
all_data = pd.concat([global_ops, scot_NI, regional, local_ops])

#Merge with the rest of the CRC data
charity_data['organisation_number'] = charity_data['organisation_number'].astype(str)
all_data['organisation_number'] = all_data['organisation_number'].astype(str)

CRCs = pd.merge(charity_data, all_data, how='left', on='organisation_number')

#Export CSV
CRCs.to_csv(f'{DATA_DIR}/outputs/CRCs.csv', index=False)