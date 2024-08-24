import pandas as pd
from utils import PROJECT_DIR
from getters import get_charities

DATA_DIR = f'{PROJECT_DIR}/'

#Upload data
charities = get_charities(DATA_DIR)

#Create uppercase charity activities column 
charities['charity_activities'] = charities['charity_activities'].str.upper()

highbrow = ['THEATRE', 'MUSEUM', 'GALLERY', 'OPERA', 'BALLET', 'CLASSICAL', 'SYMPHONY', 'PHILHARMONIC']

#Function to check if any of the strings appear in name or activties column
def check_high_culture(row):
    for col in ['charity_name', 'charity_activities']:
        if any(substring in row[col] for substring in highbrow):
            return True
        else:
            return False

#Create a new Boolean column indicating if any of the substrings appear
charities['highbrow'] = charities.apply(check_high_culture, axis=1)

#Export ids as text file
matches = charities.loc[(charities['highbrow'])]
matches = matches['organisation_number'].drop_duplicates()
matches.to_csv(f'{DATA_DIR}/other_data_sources/high_culture.txt', header=True, index=False)