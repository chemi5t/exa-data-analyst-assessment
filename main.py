import os
import pandas as pd

from decouple import config  # For managing sensitive information
from _01_emis_data_analysis_project_files.data_extraction import DataExtractor as de
from _01_emis_data_analysis_project_files.database_utils import DatabaseConnector as dco

def print_dataframe_info(df: pd.DataFrame):
    """
    Prints the DataFrame table, its info, and description.

    Parameters:
    df (pandas.DataFrame): The DataFrame to be printed and analyzed.

    Returns:
    None
    """
    print("\nDataFrame table: \n")
    print(df)
    
    print("\nDataFrame.info(): \n")
    print(df.info()) 
    
    print("\nDataFrame.describe(): \n")
    print(df.describe())

if __name__ == "__main__":

    # Initialise instances
    data_extractor = de()
    data_connector = dco()

    # Read credentials from environment file
    cred_config_access = config('credentials_env')  # Refers to .yaml file via decouple
    cred_config_api = data_connector.read_db_creds(file_path=cred_config_access)  # Extract credentials from .yaml file

    # Initialise database engine
    postgres_engine = data_connector.init_db_engine(credentials=cred_config_api)

    print("################################################## Medication Extraction ##################################################")

    # Define folder path and header file for medication data
    folder_path = os.path.join('data', 'medication')
    header_file = 'medication.csv'

    # Read and combine CSV files
    medication_df = data_extractor.read_and_combine_csv(folder_path, header_file)

    # Save the combined DataFrame to a new CSV file
    medication_df.to_csv(os.path.join('data', 'combined_medication.csv'), index=False, encoding='utf-8')
    print("\nNote: Created 'combined_medication.csv' and saved to 'data' folder.\n")

    print_dataframe_info(medication_df)

    print("################################################## Observation Extraction ##################################################")

    # Define folder path and header file for observation data
    folder_path = os.path.join('data', 'observation')
    header_file = 'observation.csv'

    # Read and combine CSV files
    observation_df = data_extractor.read_and_combine_csv(folder_path, header_file)

    # Save the combined DataFrame to a new CSV file
    observation_df.to_csv(os.path.join('data', 'combined_observation.csv'), index=False, encoding='utf-8')
    print("\nNote: Created 'combined_observation.csv' and saved to 'data' folder.\n")

    print_dataframe_info(observation_df)

    print("################################################## Clinical Extraction ##################################################")

    # Define path to clinical codes CSV file
    clinical_codes_file_path = os.path.join('data', 'clinical_codes.csv')

    # Extract clinical codes from CSV file
    clinical_codes_df = data_extractor.extract_from_csv(clinical_codes_file_path)

    print_dataframe_info(clinical_codes_df)

    print("################################################## Patient Extraction ##################################################")

    # Define path to patient CSV file
    patient_file_path = os.path.join('data', 'patient.csv')

    # Extract patient data from CSV file
    patient_df = data_extractor.extract_from_csv(patient_file_path)

    print_dataframe_info(patient_df)
