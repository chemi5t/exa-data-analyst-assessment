import pandas as pd

class DataCleaning:
    """
    A class containing static methods to clean different types of data.
    """
    @staticmethod
    def clean_column_data(selected_table_df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """
        Cleans a specified column in a DataFrame.

        The method treats the specified column as a string, replaces 'nan' strings with actual NaN values,
        and converts the column to a categorical type.

        Args:
            selected_table_df (pd.DataFrame): The DataFrame containing data.
            column_name (str): The name of the column to be cleaned.

        Returns:
            pd.DataFrame: DataFrame with the cleaned specified column.
        """
        # Treat the column as string
        selected_table_df[column_name] = selected_table_df[column_name].astype(str)

        # Replace 'nan' strings with actual NaN values
        selected_table_df.loc[selected_table_df[column_name] == 'nan', column_name] = pd.NA

        # Convert the column to categorical type
        selected_table_df[column_name] = selected_table_df[column_name].astype('category')

        return selected_table_df

    # # If is is anticipate the need to clean more columns or perform additional specific cleaning operations, having a method like clean_combined_observation_data can be useful for encapsulating these operations.
    # @staticmethod
    # def clean_combined_observation_data(selected_table_df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Cleans the 'comparator' column in the DataFrame.

    #     This method uses 'clean_column_data' to clean the 'comparator' column by treating it as a string,
    #     replacing 'nan' strings with actual NaN values, and converting the column to a categorical type.

    #     Args:
    #         selected_table_df (pd.DataFrame): The DataFrame containing observation data.

    #     Returns:
    #         pd.DataFrame: DataFrame with the cleaned 'comparator' column.
    #     """
    #     return DataCleaning.clean_column_data(selected_table_df, 'comparator')
