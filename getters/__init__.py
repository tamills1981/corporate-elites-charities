import os
import pandas as pd
  
def get_fame_data(data_dir: str) -> pd.DataFrame:
    """Gets all csv files in the folder and cleans/normalizes columns names
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        List of dataframes
    """
    
    #Empty list to store dataframea
    dataframes = []

    # Loop through all files in the directory
    for file_name in os.listdir(data_dir):
        
        # Construct the full path to the CSV file
        file_path = os.path.join(data_dir, file_name)

        # Read the CSV file into a dataframe
        df = pd.read_csv(file_path)
        
        #Drop the first column of the dataframe
        df = df.iloc[:, 1:]
        
        #Clean/normalize column names
        df.columns = df.columns.str.lower()
        df.columns = [x.replace(" ", "_") for x in df.columns.to_list()]
        df.columns = [x.replace("\n", "") for x in df.columns.to_list()]
        df.columns = [x.replace("operating_revenue_(turnover)th_gbp", "turnover") for x in df.columns]
        df.rename(columns = {'registered_number':'company_no'}, inplace=True)

        # Append the dataframe to the list
        dataframes.append(df)
    
    return dataframes

def get_companies(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of large UK companies compiled from FAME data
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Company names, numbers etc
    """
    return (pd.read_csv(f"{data_dir}/outputs/companies.csv"))

def get_appointments(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of appointments of largest companies scraped from 
    Companies House
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Appointments data
    """
    return (pd.read_csv(f"{data_dir}/outputs/dir_info.csv"))

def get_corporate_elite(data_dir: str) -> pd.DataFrame:
    """Gets dataframe of appointments of corporate elite
        
        Args:
            data_dir (str): Directory to data.

    Returns:
        pd.DataFrame: Appointments data
    """
    return (pd.read_csv(f"{data_dir}/outputs/corporate_elite.csv"))

