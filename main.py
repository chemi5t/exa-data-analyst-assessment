import os

from _01_emis_data_analysis_project_files.data_extraction import DataExtractor

if __name__ == "__main__":
    extractor = DataExtractor()

    print("################################################## medication extraction ##################################################")

    # Define the folder containing the CSV files and the header file name
    folder_path = os.path.join('data', 'medication')
    header_file = 'medication.csv'

    # Read and combine CSV files
    medication_df = extractor.read_and_combine_csv(folder_path, header_file)

    # Save the combined DataFrame to a new CSV file
    medication_df.to_csv(os.path.join('data', 'combined_medication.csv'), index=False, encoding='utf-8')

    print("\nNote: Created 'combined_medication.csv' and saved to 'data' folder.\n")

    print("\nmedication_df table: \n")
    print(medication_df)
    print("\nmedication_df.info(): \n")
    print(medication_df.info())
    print("\nmedication_df.describe(): \n")
    print(medication_df.describe())

    print("################################################## observation extraction ##################################################")

    # NOTE: following error was noted [C:\Users\chemi\AiCore_Projects\exa-data-analyst-assessment\_01_emis_data_analysis_project_files\data_extraction.py:45: DtypeWarning: Columns (2) have mixed types. Specify dtype option on import or set low_memory=False.]
    #       Due to this, dtype was inferred during the extracting of all the .csv files. Note the issue was within the observation records column 2. I would think it needs to be an integer.

    # Define the folder containing the CSV files and the header file name
    folder_path = os.path.join('data', 'observation')
    header_file = 'observation.csv'

    # Read and combine CSV files
    observation_df = extractor.read_and_combine_csv(folder_path, header_file)

    # Save the combined DataFrame to a new CSV file
    observation_df.to_csv(os.path.join('data', 'combined_observation.csv'), index=False, encoding='utf-8')

    print("\nNote: Created 'combined_observation.csv' and saved to 'data' folder.\n")

    print("\nobservation_df table: \n")
    print(observation_df)
    print("\nobservation_df.info(): \n")
    print(observation_df.info())
    print("\nobservation_df.describe(): \n")
    print(observation_df.describe())

    print("################################################## clinical extraction ##################################################")

    # Define the path to the clinical codes CSV file
    clinical_codes_file_path = os.path.join('data', 'clinical_codes.csv')

    # Extract clinical_codes from the CSV file into a pandas DataFrame
    clinical_codes_df = extractor.extract_from_csv(clinical_codes_file_path)

    print("\nclinical_codes_df table: \n")
    print(clinical_codes_df)
    print("\nclinical_codes_df.info(): \n")
    print(clinical_codes_df.info())
    print("\nclinical_codes_df.describe(): \n")
    print(clinical_codes_df.describe())

    print("################################################## patient extraction ##################################################")

    # Define the path to the CSV file
    patient_file_path = os.path.join('data', 'patient.csv')

    # Extract data from the CSV file into a pandas DataFrame
    patient_df = extractor.extract_from_csv(patient_file_path)

    print("\npatient_df table: \n")
    print(patient_df)
    print("\npatient_df.info(): \n")
    print(patient_df.info())
    print("\npatient_df.describe(): \n")
    print(patient_df.describe())
