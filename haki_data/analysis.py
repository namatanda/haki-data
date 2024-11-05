# Import files
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Union, Optional
import logging
from . logging_config import logger


def add_case_age(df: pd.DataFrame, filed_date_column: str = 'filed_date', activity_date_column: str = 'activity_date', age_column: str = 'case_age') -> pd.DataFrame:
    """
    Add a column representing the age of cases in days to the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame containing case data.
        filed_date_column (str): The column name containing the filed dates.
        activity_date_column (str): The column name containing the activity dates.
        age_column (str): The name of the new column to store the case age in days.

    Returns:
        pd.DataFrame: The DataFrame with the new 'case_age' column added.
    
    Raises:
        ValueError: If the required columns are missing in the DataFrame.
    """
    # Check if required columns exist
    required_columns = [filed_date_column, activity_date_column]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {', '.join(missing_columns)}")
        raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
    
    # Calculate the case age in days
    try:
        df[age_column] = (df[activity_date_column] - df[filed_date_column]).dt.days
        
        logger.info(f"Successfully added '{age_column}' column to the DataFrame.")
    
    except Exception as e:
        logger.error(f"Error calculating case age: {e}")
        raise

    return df

def categorize_case(case_type: str, criminal_cases: Optional[List[str]]) -> str:
    """
    Categorize a case as 'Criminal' or 'Civil' based on its type.
    
    Args:
        case_type (str): The type of the case.
        criminal_cases (Optional[List[str]]): List of case types considered as criminal.
        
    Returns:
        str: 'Criminal' if the case type is in the criminal cases list or if criminal_cases is None, 'Civil' otherwise.
    """
    if criminal_cases is None:
        return 'Criminal'
    else:
        return 'Criminal' if case_type in criminal_cases else 'Civil'

def add_case_nature(df: pd.DataFrame, criminal_cases: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Categorize all cases in the DataFrame as 'Criminal' or 'Civil'.
    
    Args:
        df (pd.DataFrame): The input DataFrame containing case data.
        criminal_cases (Optional[List[str]]): List of case types considered as criminal.
            If None, all cases are categorized as 'Criminal'.
        
    Returns:
        pd.DataFrame: DataFrame with an added 'nature' column indicating case nature.
    """
    df['nature'] = df['case_type'].apply(lambda x: categorize_case(x, criminal_cases))

    # Check for presence of both case types
    if 'Criminal' not in df['nature'].values:
        logging.warning("No criminal cases found in the DataFrame.")
    if 'Civil' not in df['nature'].values:
        logging.warning("No civil cases found in the DataFrame.")
    
    return df

def find_matching_keys(value: Any, mapping: Dict[str, Union[str, List[Any]]]) -> Optional[Union[str, List[str]]]:
    """
    Find all keys in a dictionary where the given value is present.

    Args:
        value (Any): The value to search for.
        mapping (Dict[str, Union[str, List[Any]]]): A dictionary where keys map to either a string or a list of values.

    Returns:
        Optional[Union[str, List[str]]]: A single key if exactly one match is found, 
                                         a list of keys if multiple matches are found,
                                         or None if no matches are found.
    """
    matching_keys = [
        key for key, dict_value in mapping.items()
        if (isinstance(dict_value, str) and dict_value == value) or
           (isinstance(dict_value, list) and value in dict_value)
    ]

    if not matching_keys:
        return None
    return matching_keys[0] if len(matching_keys) == 1 else matching_keys



def add_broad_category(
    df: pd.DataFrame, 
    case_type_column: str, 
    broad_case_type_column: str, 
    mapping: Dict[str, Union[str, List[Any]]]
) -> pd.DataFrame:
    """
    Map case types to broad case categories based on a given mapping dictionary.

    Args:
        df (pd.DataFrame): The DataFrame containing the data.
        case_type_column (str): The column name containing case types to be mapped.
        broad_case_type_column (str): The name of the new column where the broad case categories will be stored.
        mapping (Dict[str, Union[str, List[Any]]]): The mapping dictionary where keys are broad categories and values are specific case types.

    Returns:
        pd.DataFrame: The DataFrame with the new column containing broad case categories.
    
    Raises:
        ValueError: If the case_type_column is not found in the DataFrame.
    """
    # Check if the required column exists
    if case_type_column not in df.columns:
        logger.error(f"Column '{case_type_column}' not found in DataFrame")
        raise ValueError(f"Column '{case_type_column}' not found in the DataFrame")
    
    # Apply the dictionary mapping using find_matching_keys function
    df[broad_case_type_column] = df[case_type_column].apply(lambda x: find_matching_keys(x, mapping))
    
    logger.info(f"Successfully mapped case types to broad categories in '{broad_case_type_column}' column.")
    return df


def analyze_court_outcomes(df: pd.DataFrame, start_date: str, end_date: str, outcome: str) -> pd.DataFrame:
    """
    Calculate the number of case outcomes per court within a specified period.
    
    Args:
        df (pd.DataFrame): A pandas DataFrame containing the data.
        start_date (str): The starting date of the period (YYYY-MM-DD format).
        end_date (str): The ending date of the period (YYYY-MM-DD format).
        outcome (str): A column representing the outcome of interest.
        
    Returns:
        pd.DataFrame: A DataFrame showing the number of resolved cases per court and case category.
    """
    try:
        period_start = pd.to_datetime(start_date)
        period_end = pd.to_datetime(end_date)
        
        if period_start > period_end:
            raise ValueError("start_date must be earlier than end_date")
        
        required_columns = {'court', 'broad_case_type', 'activity_date', outcome}
        if not required_columns.issubset(df.columns):
            missing_columns = required_columns - set(df.columns)
            raise KeyError(f"Missing required columns: {missing_columns}")
        
        filtered_cases = df[
            (df['activity_date'] >= period_start) &
            (df['activity_date'] <= period_end) &
            (df[outcome] == 1)
        ]
        
        if filtered_cases.empty:
            logging.warning("No cases found for the given date range and outcome.")
   
        outcome_by_type = (
            filtered_cases
            .groupby(['court', 'broad_case_type'])
            .size()
            .reset_index(name='num_cases')
        )

        result = outcome_by_type.pivot_table(
            index='court', 
            columns='broad_case_type', 
            values='num_cases', 
            fill_value=0
        )
        
        logging.info("Successfully calculated case outcomes per court.")
        return result
    
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        raise


def process_case_time_limits(df: pd.DataFrame, time_limits: Dict[str, int]) -> pd.DataFrame:
    """
    Process the case data by adding age and time limit compliance columns.

    Args:
        df (pd.DataFrame): The input DataFrame containing case data.
                           Required columns: 'filed_date', 'activity_date', 'broad_case_type', 'concluded'
        time_limits (Dict[str, int]): A dictionary with case categories as keys and time limits as values.

    Returns:
        pd.DataFrame: The processed DataFrame with 'age' and 'within_time_limit' columns added.
    """

    # Check if each case is within the time limit and concluded
    df['within_time_limit'] = (
        (df['age'] <= df['broad_case_type'].map(time_limits).fillna(0)) & 
        df['concluded']
    )
    
    return df


def is_concluded(outcome: str, resolved_outcomes: List[str]) -> bool:
    """
    Determine if a case outcome is resolved.

    Args:
        outcome (str): The outcome of the case.
        resolved_outcomes (List[str]): List of outcomes that indicate resolution.

    Returns:
        bool: True if the outcome is considered resolved, otherwise False.
    """
    return outcome.lower() in (resolved.lower() for resolved in resolved_outcomes)


def is_case_registered(outcome: str, activity_date: Union[str, pd.Timestamp], filed_date: Union[str, pd.Timestamp]) -> bool:
    """
    Check if a case is registered based on outcome and whether activity and filed dates match.

    Args:
        outcome (str): The outcome of the case.
        activity_date (Union[str, pd.Timestamp]): The date of the activity.
        filed_date (Union[str, pd.Timestamp]): The date the case was filed.

    Returns:
        bool: True if the outcome implies registration and activity_date matches filed_date, otherwise False.
    """
    outcome = outcome.strip().lower()
    
    if 'registered' not in outcome and 'filed' not in outcome:
        return False

    return pd.notna(activity_date) and pd.notna(filed_date) and activity_date == filed_date



def add_conclusion(df: pd.DataFrame, resolved_outcomes: List[str]) -> pd.DataFrame:
    """
    Add the 'concluded' column to the DataFrame based on resolved outcomes.
    
    Args:
        df (pd.DataFrame): The DataFrame with case data.
        resolved_outcomes (List[str]): List of outcomes considered resolved.
    
    Returns:
        pd.DataFrame: DataFrame with 'concluded' column added.
    """
    df['concluded'] = df['outcome'].apply(lambda outcome: is_concluded(outcome, resolved_outcomes))
    return df


def add_registration(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add the 'registered' column to the DataFrame based on case registration criteria.
    
    Args:
        df (pd.DataFrame): The DataFrame with case data.
    
    Returns:
        pd.DataFrame: DataFrame with 'registered' column added.
    """
    df['registered'] = df.apply(
        lambda row: is_case_registered(row['outcome'], row['activity_date'], row['filed_date']),
        axis=1
    )
    return df

def add_productivity(df: pd.DataFrame, merit_outcomes: list) -> pd.DataFrame:
    """
    Categorize each row in the DataFrame based on merit and conclusion status.

    Args:
        df (pd.DataFrame): The input DataFrame containing 'outcome' and 'concluded' columns.
        merit_outcomes (list): A list of outcomes considered merit outcomes.

    Returns:
        pd.DataFrame: The DataFrame with an additional 'productivity_category' column.
    """
    def categorize_row(row):
        if row['outcome'] in merit_outcomes and row['concluded'] == 1:
            return 'merit'
        elif row['outcome'] not in merit_outcomes and row['concluded'] == 1:
            return 'non-merit'
        else:
            return None
    df = add_case_nature(df, CRIMINAL_CASES)
    df = add_conclusion(df, RESOLVED_OUTCOMES)
    # Apply the row categorization
    df['productivity_category'] = df.apply(categorize_row, axis=1)
    
    return df

def get_productivity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a pivot table summarizing the productivity category counts by court.

    Args:
        df (pd.DataFrame): The input DataFrame with a 'productivity_category' column.

    Returns:
        pd.DataFrame: The pivot table with merit and non-merit case counts for each court.
    """
    # Create the pivot table
    productivity_pivot_table = pd.pivot_table(
        df,
        values='concluded',  
        index='court',     
        columns='productivity_category', 
        aggfunc='count',   
        fill_value=0        
    ).rename(columns={'merit': 'Merit', 'non-merit': 'Non_Merit'})

    return productivity_pivot_table

def calculate_adjournment_proportion(df: pd.DataFrame, non_adjournable: list[str]) -> pd.DataFrame:
    """
    Perform adjournment analysis on the DataFrame by creating columns for adjourned and adjournable events,
    calculating adjourned events per court and reason, and determining adjournment rates.

    Args:
        df (pd.DataFrame): The input DataFrame containing 'reason_adj', 'comingfor', and 'court' columns.
        non_adjournable (list): A list of 'comingfor' values that are considered non-adjournable.

    Returns:
        pd.DataFrame: A DataFrame containing adjournment proportions per court.
    """
    if not all(col in df.columns for col in ['reason_adj', 'comingfor', 'court']):
        raise ValueError("Input DataFrame must contain 'reason_adj', 'comingfor', and 'court' columns.")

    df['comingfor'] = df['comingfor'].str.strip()

    # 1. Create 'adjourned' column (1 if 'reason_adj' is not null and 'comingfor' is not in non_adjournable, else 0)
    df['adjourned'] = (df['reason_adj'].notnull() & df['comingfor'].apply(lambda x: x not in non_adjournable)).astype(int)
def get_monthly_case_stats(df, registered_col, concluded_col):
    """Calculates monthly statistics for registered and concluded cases.

    Args:
        df (pandas.DataFrame): The input DataFrame containing case data.
        registered_col (str): The name of the column containing registered cases.
        concluded_col (str): The name of the column containing concluded cases.

    Returns:
        pandas.DataFrame: A DataFrame with monthly statistics for registered and concluded cases.
    """

    monthly_cases = df.groupby(['court', 'date_mon', 'case_type']).agg(
        registered=(registered_col, 'sum'),
        concluded=(concluded_col, 'sum')
    ).reset_index()

    return monthly_cases
    # 2. Create 'adjournable' column (1 if 'comingfor' is not in non_adjournable, else 0)
    df['adjournable'] = df['comingfor'].apply(lambda x: x not in non_adjournable).astype(int)

    # 3. Calculate adjourned events per court and reason_adj
    adjourned_per_court_reason = df.groupby(['court', 'reason_adj'])['adjourned'].sum().reset_index(name='count')

    # 4. Sum adjourned and adjournable events per court
    adjourned = df.groupby('court')['adjourned'].sum().reset_index(name='total_adjourned')
    adjournable = df.groupby('court')['adjournable'].sum().reset_index(name='total_adjournable')

    # 5. Calculate the adjournment proportion per court
    adjourn_proportion = pd.merge(adjourned, adjournable, on='court')
    adjourn_proportion['adjourn_proportion'] = (adjourn_proportion['total_adjourned'] / adjourn_proportion['total_adjournable']) * 100

    return adjourn_proportion

def get_monthly_case_stats(df, registered_col, concluded_col):
    """Calculates monthly statistics for registered and concluded cases.

    Args:
        df (pandas.DataFrame): The input DataFrame containing case data.
        registered_col (str): The name of the column containing registered cases.
        concluded_col (str): The name of the column containing concluded cases.

    Returns:
        pandas.DataFrame: A DataFrame with monthly statistics for registered and concluded cases.
    """

    monthly_cases = df.groupby(['court', 'date_mon', 'case_type']).agg(
        registered=(registered_col, 'sum'),
        concluded=(concluded_col, 'sum')
    ).reset_index()

    return monthly_cases

def get_cases_per_quarter(df, column):
    # Group by quarters and count cases
    cases_per_quarter = df.groupby(pd.Grouper(key='activity_date', freq='QE'))[column].sum()

    # Reset index to make the quarters a column
    cases_per_quarter = cases_per_quarter.reset_index()

    # Rename the columns
    cases_per_quarter.columns = ['quarter', f'cases_{column}']

    return cases_per_quarter


def get_quarterly_stats(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate quarterly statistics for adjourned, adjournable, concluded, and registered cases.

    Args:
        df (pd.DataFrame): The input DataFrame containing case data.

    Returns:
        pd.DataFrame: A DataFrame with quarterly statistics.
    """
    quarterly_adjourned = get_cases_per_quarter(df, 'adjourned')
    quarterly_adjournable = get_cases_per_quarter(df, 'adjournable')
    quarterly_concluded = get_cases_per_quarter(df, 'concluded')
    quarterly_registered = get_cases_per_quarter(df, 'registered')
    
    # Merge quarterly data on quarter column
    quarterly_stats = quarterly_adjourned.merge(
        quarterly_adjournable, on='quarter'
    ).merge(
        quarterly_concluded, on='quarter'
    ).merge(
        quarterly_registered, on='quarter'
    )
    
    return quarterly_stats
