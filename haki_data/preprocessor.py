import pandas as pd
import numpy as np
from .constants import *
from .utils import *
from .cleaner import *
from .analysis import * 
from .hc_helper import transform_court_names
#clean data
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = transform_court_names(df)
    df = df.rename(columns={'court_name': 'court'})
    df['outcome'] = df['outcome'].replace('Terminated/ Struck Out/ Dismissed/Case Closed', 'Terminated')
    df = drop_nan_columns(df, ['date_dd', 'date_mon', 'date_yyyy', 'caseid_type', 'caseid_no', 'filed_dd', 'filed_mon', 'filed_yyyy', 'case_type', 'comingfor'])
    df = remove_duplicates(df)
    df = drop_null_values(df)
    df = strip_dataframe_columns(df)
    df = convert_to_title_case(df, 'outcome')

    return df

# transform data
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    
    try:
        validate_columns(df, ['outcome', 'activity_date', 'filed_date'])
        logger.info("Validation passed.")
    except ValueError as e:
        logger.error(e)
    df = add_date(df.copy(), ['date_dd', 'date_mon', 'date_yyyy'], 'activity_date')
    df = add_date(df.copy(), ['filed_dd', 'filed_mon', 'filed_yyyy'], 'filed_date')
    df = add_date(df.copy(), ['next_dd', 'next_mon','next_yyyy'], 'next_date')
    df = add_case_number(df, 'court', 'caseid_type', 'caseid_no', 'filed_yyyy')
    df = add_case_age(df)


    df = add_case_nature(df, CRIMINAL_CASES)
    df = add_conclusion(df, RESOLVED_OUTCOMES)
    df = add_registration(df)
    df = add_productivity(df, MERIT_OUTCOMES)
    df = add_broad_category(df, 'case_type', 'broad_case_type', BROAD_CASE_TYPES)
  
    return df
