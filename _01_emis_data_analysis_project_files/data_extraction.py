import csv
import os
import pandas as pd

class DataExtractor:
    """
    A utility class for extracting data from various sources.
    """

    def __init__(self):
        pass

    def extract_from_csv(self, file_path: str) -> pd.DataFrame:
        """
        Extract data from a CSV file.

        Args:
            file_path (str): Path to the CSV file.

        Returns:
            df (pd.DataFrame): Extracted data as a pandas DataFrame.
        """
        # Read CSV data into a pandas DataFrame
        df = pd.read_csv(file_path, encoding='utf-8')
        return df

if __name__ == "__main__":
    extractor = DataExtractor()

    # Define the path to the CSV file
    patient_file_path = os.path.join(r'..\data', 'patient.csv')

    # Extract data from the CSV file into a pandas DataFrame
    patient_df = extractor.extract_from_csv(patient_file_path)

    print("patient_df table: \n")
    print(patient_df)
    print("\npatient_df.info(): \n")
    print(patient_df.info())
    print("\npatient_df.describe(): \n")
    print(patient_df.describe())

    # Define the path to the CSV file
    clinical_codes_file_path = os.path.join(r'..\data', 'clinical_codes.csv')

    # Extract data from the CSV file into a pandas DataFrame
    clinical_codes_df = extractor.extract_from_csv(clinical_codes_file_path)

    print("\nclinical_codes_df table: \n")
    print(clinical_codes_df)
    print("\nclinical_codes_df.info(): \n")
    print(clinical_codes_df.info())
    print("\nclinical_codes_df.describe(): \n")
    print(clinical_codes_df.describe())
