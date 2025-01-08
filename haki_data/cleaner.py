import pandas as pd
import numpy as np
from typing import Dict, Any, List, Union, Optional

from . logging_config import logger

def drop_nan_columns(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Drop rows containing NaN values from the specified columns of a DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame to process.
        columns (List[str]): A list of column names to check for NaN values.

    Returns:
        pd.DataFrame: The updated DataFrame with NaN-containing rows dropped.

    Raises:
        ValueError: If any of the specified columns are not present in the DataFrame.
    """
    # Validate that all specified columns exist in the DataFrame
    missing_columns = set(columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Columns not found in DataFrame: {', '.join(missing_columns)}")

    # Identify columns with NaN values
    nan_columns = df[columns].columns[df[columns].isna().any()].tolist()

    # Log dropped rows if any
    if nan_columns:
        nan_count = df[columns].isna().sum()
        logger.info("Dropping rows with NaN values:")
        for col in nan_columns:
            logger.info(f"  {col}: {nan_count[col]} rows")

    # Drop rows with NaN values in specified columns
    original_row_count = len(df)
    df_cleaned = df.dropna(subset=columns)
    dropped_row_count = original_row_count - len(df_cleaned)

    if dropped_row_count > 0:
        logger.info(f"Total rows dropped: {dropped_row_count}")
    else:
        logger.info("No rows were dropped.")

    return df_cleaned

def remove_duplicates(data: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicates from a DataFrame.
    
    Args:
        data (pd.DataFrame): Input DataFrame.
        
    Returns:
        pd.DataFrame: DataFrame with duplicates removed.
    """
    num_duplicates = data.duplicated().sum()
    
    if num_duplicates > 0:
        logging.info(f"{num_duplicates} duplicates found.")
        data = data.drop_duplicates(keep="first").reset_index(drop=True)
        logging.info(f"{num_duplicates} duplicates removed.")
    else:
        logging.info("No duplicates found.")
    
    return data


def drop_null_values(df: pd.DataFrame, column_name: str = 'outcome') -> pd.DataFrame:
    """
    Drop rows from the DataFrame where the specified column contains null values.

    Args:
        df (pd.DataFrame): The DataFrame from which to drop rows.
        column_name (str): The name of the column to check for null values. Default is 'outcome'.

    Returns:
        pd.DataFrame: The DataFrame with rows containing null values in the specified column dropped.
    """
    df['outcome'] = df['outcome'].str.strip()
    initial_row_count: int = df.shape[0]
    cleaned_df: pd.DataFrame = df.dropna(subset=[column_name])
    final_row_count: int = cleaned_df.shape[0]
    dropped_row_count: int = initial_row_count - final_row_count
    
    if dropped_row_count > 0:
        logger.info(f"Total dropped rows with null values in '{column_name}': {dropped_row_count}")
    else:
        logger.info(f"No rows dropped with null values in '{column_name}'")
    return cleaned_df


def strip_dataframe_columns(df):
    """Strips leading and trailing whitespace from all columns in a Pandas DataFrame.

    Args:
        df (pandas.DataFrame): The DataFrame to modify.

    Returns:
        pandas.DataFrame: The modified DataFrame with stripped columns.
    """

    try:
        df = df.astype(str).apply(lambda x: x.str.strip())
        logger.info("str.strip() applied successfully to all columns.")
        return df
    except Exception as e:
        logger.error(f"Error applying str.strip(): {e}")
        return None



def apply_title_case(text, column):
    """
    Convert text to title case. Handle non-string values and nulls.

    Args:
        text: The value from the DataFrame to be processed.
        column (str): The name of the column (for logging purposes).

    Returns:
        The text in title case or the original value if not a string.
    """
    if pd.isna(text):
        return np.nan
    if not isinstance(text, str):
        logger.warning(f"Non-string value encountered in '{column}': {text}")
        return str(text)
    return text.title()

def convert_to_title_case(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Process the specified column of the DataFrame by applying title case.

    Args:
        df (pd.DataFrame): The input DataFrame.
        column (str): The name of the column to process.

    Returns:
        pd.DataFrame: The DataFrame with the processed column.
    """

    if column not in df.columns:
        logger.error(f"'{column}' column not found in the DataFrame")
        return df

    original_null_count = df[column].isnull().sum()

    # Apply the title case function to the column
    df[column] = df[column].apply(lambda x: apply_title_case(x, column))


    return df

    # new_null_count = df[column].isnull().sum()
    # if new_null_count > original_null_count:
    #     logger.warning(f"Number of null values in '{column}' increased from {original_null_count} to {new_null_count}")

    # non_string_count = df[column].apply(lambda x: not isinstance(x, str) if pd.notna(x) else False).sum()
    # if non_string_count > 0:
    #     logger.warning(f"Found {non_string_count} non-string values in '{column}' after processing")
