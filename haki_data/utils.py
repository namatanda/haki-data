import pandas as pd
import os
from multiprocessing import Pool
import logging
import numpy as np
from typing import Dict, Any, List, Union, Optional
from . logging_config import logger

def validate_columns(df: pd.DataFrame, required_columns: Union[str, List[str]]) -> None:
    """
    Validate that the DataFrame contains the required columns.

    Args:
        df (pd.DataFrame): The input DataFrame.
        required_columns (Union[str, List[str]]): A single column name (str) or a list of column names (List[str]) that must be present.

    Raises:
        ValueError: If any required columns are missing.
    """
    # Convert required_columns to a list if it's a string
    if isinstance(required_columns, str):
        required_columns = [required_columns]

    # Check which required columns are missing
    missing_columns = set(required_columns) - set(df.columns)
    
    # If there are missing columns, raise a ValueError with an informative message
    if missing_columns:
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")



def add_date(df: pd.DataFrame, column_names: List[str], new_col: str) -> pd.DataFrame:
    """
    Creates a new date column in the DataFrame by concatenating the values of three specified columns.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        column_names (List[str]): A list of three column names to be concatenated [year, month, day].
        new_col (str): The import commandsname of the new date column to be created.

    Returns:
        pd.DataFrame: The DataFrame with the new date column added.

    Raises:
        ValueError: If the input list doesn't contain exactly three column names or if columns are missing.
    """
    if len(column_names) != 3:
        raise ValueError("column_names must contain exactly three elements: [year, month, day]")

    year_col, month_col, day_col = column_names

    # Check if all required columns exist in the DataFrame
    missing_columns = set(column_names) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in DataFrame: {', '.join(missing_columns)}")

    # Create copies to avoid SettingWithCopyWarning
    df = df.copy()

    try:
        # Convert year and day columns to integers
        df[year_col] = df[year_col].astype(float).astype(int)
        df[day_col] = df[day_col].astype(float).astype(int)

        # Concatenate the columns to create a date string
        df[new_col] = (df[year_col].astype(str) + '-' + 
                       df[month_col].astype(str) + '-' + 
                       df[day_col].astype(str))

        # Convert to datetime
        df[new_col] = pd.to_datetime(df[new_col], errors='coerce')

        # Log information about the conversion
        valid_dates = df[new_col].notna().sum()
        logger.info(f"Created new date column '{new_col}'. Valid dates: {valid_dates}/{len(df)}")

    except Exception as e:
        logger.error(f"Error creating date column: {str(e)}")
        raise

    return df

def add_case_number(df: pd.DataFrame, court_col: str, caseid_type_col: str, caseid_no_col: str, filed_yyyy_col: str, new_col='case_number') -> pd.DataFrame:
    """
    Generates a case number by concatenating court, caseid_type, caseid_no, and filed_yyyy columns.

    Args:
        df (pd.DataFrame): The dataframe containing the necessary columns.
        court_col (str): The name of the column containing court information.
        caseid_type_col (str): The name of the column containing case ID type.
        caseid_no_col (str): The name of the column containing case ID number.
        filed_yyyy_col (str): The name of the column containing the year the case was filed.
        new_col (str): The name of the new column to be created for the case number. Default is 'case_num'.

    Returns:
        pd.DataFrame: DataFrame with the new case number column.
    """
    df[new_col] = df[court_col] + '/' + df[caseid_type_col] + '/' + df[caseid_no_col] + '/' + df[filed_yyyy_col].astype(str)
    return df




