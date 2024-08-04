import os
import pandas as pd

from decouple import config  # For managing sensitive information
from _01_emis_data_analysis_project_files.database_utils import DatabaseConnector as dco
from _01_emis_data_analysis_project_files.data_cleaning import DataCleaning as dcl
from _01_emis_data_analysis_project_files.data_extraction import DataExtractor as de

print("################################################## Initialise instances ##################################################")

# Initialise instances
data_extractor = de()
data_connector = dco()
data_cleaning = dcl()  

# Read credentials from environment file
cred_config_access = config('credentials_env')  # Refers to .yaml file via decouple
cred_config_api = data_connector.read_db_creds(file_path=cred_config_access)  # Extract credentials from .yaml file

# Initialise database engine
postgres_engine = data_connector.init_db_engine(credentials=cred_config_api)

def print_dataframe_info(df: pd.DataFrame):
    """
    Prints the pd.DataFrame table, its info, and description.

    Parameters:
    df (pd.DataFrame): The pd.DataFrame to be printed and analysed.

    Returns:
    None
    """
    print("\nDataFrame table: \n")
    print(df)
    
    print("\nDataFrame.info(): \n")
    print(df.info()) 
    
    print("\nDataFrame.describe(): \n")
    print(df.describe())

def medication_extraction():
    # Define folder path and header file for medication data
    folder_path = os.path.join('data', 'medication')
    header_file = 'medication.csv'

    # Read and combine CSV files
    medication_df = data_extractor.read_and_combine_csv(folder_path, header_file)

    # Save the combined DataFrame to a new CSV file
    medication_df.to_csv(os.path.join('data', 'combined_medication.csv'), index=False, encoding='utf-8')
    print("\nNote: Created 'combined_medication.csv' and saved to 'data' folder.\n")

    print_dataframe_info(medication_df)

    return medication_df

def observation_extraction_transformation():

    # Define folder path and header file for observation data
    folder_path = os.path.join('data', 'observation')
    header_file = 'observation.csv'

    # Read and combine CSV files
    observation_df = data_extractor.read_and_combine_csv(folder_path, header_file)

    # Save the combined DataFrame to a new CSV file
    observation_df.to_csv(os.path.join('data', 'combined_observation.csv'), index=False, encoding='utf-8')
    print("\nNote: Created 'combined_observation.csv' and saved to 'data' folder.\n")

    # Clean the 'comparator' column in the 'observation' DataFrame using the DataCleaning class.
    # The method ensures that the column is treated as a string, replaces 'nan' strings with actual NaN values,
    # and converts the column dtype to 'category' from 'object' to handle mixed dtype issues.
    observation_df = data_cleaning.clean_column_data(observation_df, 'comparator')

    print("The 'comparator' column has been cleaned using the 'data_cleaning' method. The dtype has been cast to 'category' from 'object' to resolve any mixed dtype issues.\n")

    print_dataframe_info(observation_df)

    return observation_df

def clinical_extraction():
    # Define path to clinical codes CSV file
    clinical_codes_file_path = os.path.join('data', 'clinical_codes.csv')

    # Extract clinical codes from CSV file
    clinical_codes_df = data_extractor.extract_from_csv(clinical_codes_file_path)

    print_dataframe_info(clinical_codes_df)

    return clinical_codes_df

def patient_extraction():
    # Define path to patient CSV file
    patient_file_path = os.path.join('data', 'patient.csv')

    # Extract patient data from CSV file
    patient_df = data_extractor.extract_from_csv(patient_file_path)

    print_dataframe_info(patient_df)

    return patient_df

if __name__ == "__main__":
    
    try:
        print("################################################## Medication Extraction ##################################################")

        medication_df = medication_extraction()

        print("################################################## Observation Extraction and Transformation ##################################################")

        observation_df = observation_extraction_transformation()

        print("################################################## Clinical Codes Extraction ##################################################")

        clinical_codes_df = clinical_extraction()

        print("################################################## Patient Extraction ##################################################")

        patient_df = patient_extraction()

        print("################################################## Upload to postgresSQL (pgAdmin4) ##################################################")

        # List of DataFrames and their corresponding table names
        df_list_to_upload = [
            {"dataframe": medication_df, "table_name": "dim_medication"},
            {"dataframe": observation_df, "table_name": "dim_observation"},
            {"dataframe": clinical_codes_df, "table_name": "dim_clinical_codes"},
            {"dataframe": patient_df, "table_name": "dim_patient"}
        ]
                
        # Upload DataFrames to PostgreSQL
        data_connector.upload_list_to_db(df_list_to_upload, engine = postgres_engine)

        print("################################################## ETL completed ##################################################")

    except FileNotFoundError as e:
        print(f"Error: {e}")
    except ValueError as e:
        print(f"Value Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
