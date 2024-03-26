import pandas as pd
from utils import PROJECT_DIR
from getters import get_final_dataset
import numpy as np
from scipy.stats import chi2_contingency

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
df = get_final_dataset(DATA_DIR)

#Drop columns not used in analysis
columns_to_drop = ['organisation_number', 'charity_name', 'date_of_registration',
 'mean_gross_income', 'area_of_operation', 'years_not_active']
df = df.drop(columns=columns_to_drop)

df = df.dropna()

#For Table 1

# Function to generate count and percentage
def count_and_percentage(column):
    counts = column.value_counts()
    percentages = (counts / len(column)) * 100
    return pd.DataFrame({'Frequency': counts, 'Percentage': percentages})

# Creating a new DataFrame to store counts and percentages
Table_1 = pd.concat([count_and_percentage(df[col].astype(str)) for col in df.columns], axis=0, keys=df.columns)

#For Table 2

#Create list of variables
variables = df.columns.to_list()

#Drop outcome variable from list
variables.remove('corporate_elite_trustee')

chi2 = []
crosstabs_list = []

for v in variables:
    crosstab = pd.crosstab(df[v], df['corporate_elite_trustee']) 
    c, p, dof, expected = chi2_contingency(crosstab)
    result = dict()
    result['variable'] = v
    result['chi2'] = c
    result['p-value'] = p
    result['degrees of freedom'] = dof
    chi2.append(result)
    
    crosstab = crosstab.reset_index()
    crosstab_percent = pd.crosstab(df[v], df['corporate_elite_trustee'], normalize='index').reset_index()  
    crosstabs = pd.concat([crosstab_percent, crosstab], axis=1).rename(columns = {v:'outcome_var'})
    crosstabs.insert(0,'variable', v)
    expected = pd.DataFrame(expected, columns=['expected_false', 'expected_true'])
    crosstabs = pd.concat([crosstabs,expected], axis=1)
    
    crosstabs_list.append(crosstabs)
    
chi2_df = pd.DataFrame.from_dict(chi2) 
crosstabs_df = pd.concat(crosstabs_list).sort_values('variable').reset_index(drop=True)

#Permutation test

#Dataframe of results to populate with results of permutation test data
sim_df = (
          crosstabs_df.iloc[:, [0, 1, 6, 8]]
          .rename(columns = {'outcome_var':'value', True:'N_observed', 'expected_true':'N_expected'})
          )

#Set number of permutations
no_iterations = 10000

#While loop running permutations
iteration = 0

while iteration < no_iterations:
    iteration += 1
    
    #Random numpy array with same frequency as the outome variable
    random_rows = np.array([True] * 983 + [False] * 30789)
    np.random.shuffle(random_rows)
    
    #Replace the outcome variable with the random array
    df['corporate_elite_trustee'] = random_rows
    
    #List to populate with crosstabs of simulated data
    crosstabs_list = []
    
    #run crosstabs on all predictor variables
    for v in variables:
        crosstab = pd.crosstab(df[v], df['corporate_elite_trustee']).reset_index().rename(columns = {v:'value'})
        crosstab.insert(0,'variable', v) 
        crosstab = (
                crosstab.drop(columns=False)
                .rename(columns={True: 'it_' + str(iteration)})
                )       
        crosstabs_list.append(crosstab)
    
    #Combine in single dataframe
    it_df = pd.concat(crosstabs_list)
    
    #Add as new column to the dataframe with the observed data
    sim_df = pd.merge(sim_df, it_df, how='left', on=['variable', 'value'])


# Populate dictionary with proportion of simulated results as extreme as 
# the observed data
permutation_results_dict = {}

for row in sim_df.iterrows():
    variable = row[1]['variable']
    value = row[1]['value']
    N_observed = row[1]['N_observed']
    N_expected = row[1]['N_expected']
    permutation_results = row[1][4:].to_frame(name='results')
    
    #Replace this section with squared results
    if N_observed >= N_expected:
        permutation_results['extreme'] = permutation_results > N_observed
    else:
        permutation_results['extreme'] = permutation_results < N_observed
    
    p_value = permutation_results['extreme'].sum() / 10000
    permutation_results_dict[row[0]] = {
        'variable': variable,
        'value': value,
        'p_value': p_value,
    }

#Dataframe from dictionary
permutation_results_df = pd.DataFrame.from_dict(permutation_results_dict).T

#Export permutation results and p values
permutation_results_df.to_csv(f'{DATA_DIR}/outputs/permutation_test_p_values.csv', index=False)
sim_df.to_csv(f'{DATA_DIR}/outputs/permutation_test_results.csv', index=False)