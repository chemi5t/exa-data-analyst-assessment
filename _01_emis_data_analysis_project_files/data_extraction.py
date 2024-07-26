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
    
    def read_and_combine_csv(self, folder_path: str, header_file: str) -> pd.DataFrame:
        """
        Read a CSV file with header names and combine its data with data from other CSV files
        in the specified folder (excluding the header file). The other CSV files do not have headers.
        
        Args:
            folder_path (str): Path to the folder containing CSV files.
            header_file (str): Name of the CSV file that contains the column headers (with data).
        
        Returns:
            pd.DataFrame: Combined DataFrame with data from all CSV files.
        """
        # Full path to the header file
        header_file_path = os.path.join(folder_path, header_file)
        
        if not os.path.isfile(header_file_path):
            raise FileNotFoundError(f"The header file at {header_file_path} does not exist or is not a file.")
        
        # Read the header file into a DataFrame
        df_header = pd.read_csv(header_file_path, encoding='utf-8')

        # List all CSV files in the folder, excluding the header file
        files = [f for f in os.listdir(folder_path) if f.endswith('.csv') and f != header_file]
        
        # Initialise a list to hold DataFrames
        df_list = [df_header]

        # Read and append each CSV file to the df_list
        for file in files:
            file_path = os.path.join(folder_path, file)
            # Read each file without headers (header=None) and use the same column names as df_header
            df = pd.read_csv(file_path, encoding='utf-8', header=None)
            
            # Ensure the DataFrame has the same number of columns as the header DataFrame
            if df.shape[1] != df_header.shape[1]:
                raise ValueError(f"File {file} has a different number of columns than the header file.")
            
            # Set the column names from the header DataFrame
            df.columns = df_header.columns
            
            # Append this DataFrame to the list
            df_list.append(df)

        # Concatenate all DataFrames in the list into a single DataFrame
        combined_df = pd.concat(df_list, ignore_index=True)
        
        return combined_df
