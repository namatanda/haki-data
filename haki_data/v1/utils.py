import pandas as pd
import os
from multiprocessing import Pool
import logging
from typing import List, Optional, Dict
from functools import partial

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_excel_file(file_path: str, column_names: List[str]) -> Optional[pd.DataFrame]:
    """
    Read an Excel file and return a DataFrame with specified column names and an added court name column.

    Args:
        file_path (str): Path to the Excel file.
        column_names (List[str]): List of column names to use for the DataFrame.

    Returns:
        Optional[pd.DataFrame]: DataFrame with the specified columns and court name, or None if an error occurs.
    """
    try:
        file_name = os.path.basename(file_path)
        court_name = file_name.split("-")[0]
        df = pd.read_excel(file_path, header=4, names=column_names)
        df = df.assign(court_name=court_name).drop(df.columns[0], axis=1)
        df = df[['court_name'] + list(df.columns[:-1])]
        return df
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return None

def read_excel_files(folder_path: str, column_names: List[str]) -> pd.DataFrame:
    """
    Read all Excel files in a folder and combine them into a single DataFrame.

    Args:
        folder_path (str): Path to the folder containing Excel files.
        column_names (List[str]): List of column names to use for the DataFrame.

    Returns:
        pd.DataFrame: Combined DataFrame from all Excel files.

    Raises:
        ValueError: If no Excel files are found or if unable to read any Excel files.
    """
    file_paths = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)
                  if filename.endswith((".xls", ".xlsx"))]

    if not file_paths:
        raise ValueError("No Excel files found in the specified folder.")

    with Pool() as pool:
        read_func = partial(read_excel_file, column_names=column_names)
        data_frames = pool.map(read_func, file_paths)

    data_frames = [df for df in data_frames if df is not None and not df.empty]

    if not data_frames:
        raise ValueError("Unable to read any Excel files.")

    return pd.concat(data_frames, ignore_index=True)

def process_dataframe(df: pd.DataFrame, name_map: Optional[Dict[str, str]] = None, court_prefix_to_remove: Optional[str] = None) -> pd.DataFrame:
    """
    Process the combined DataFrame by filling NaN values, optionally mapping court names, and reordering columns.

    Args:
        df (pd.DataFrame): Input DataFrame to process.
        name_map (Optional[Dict[str, str]]): Optional dictionary for mapping court names.
        court_prefix_to_remove (Optional[str]): Optional string prefix to remove from court names.

    Returns:
        pd.DataFrame: Processed DataFrame.
    """
    # Fill NaN values in float columns with 0 and convert them to int
    float_columns = df.select_dtypes(include=['float64']).columns
    df[float_columns] = df[float_columns].fillna(0).astype(int)

    if name_map:
        # Create a new column with mapped names
        def map_names(name: str) -> str:
            for key, value in name_map.items():
                name = name.replace(key, value)
            return name.split()[0]

        df['court'] = df['court_name'].apply(map_names)
        
        if court_prefix_to_remove:
            df['court'] = df['court'].str.replace(court_prefix_to_remove, "", case=False, regex=False)
        
        df['court'] = df['court'].str.replace(r'\s+', ' ', regex=True)

        # Move court name to the first column and drop the original court_name column
        df = df[['court'] + [col for col in df.columns if col != 'court' and col != 'court_name']]
    else:
        # If no name_map is provided, just rename 'court_name' to 'court'
        df = df.rename(columns={'court_name': 'court'})
        
        if court_prefix_to_remove:
            df['court'] = df['court'].str.replace(court_prefix_to_remove, "", case=False, regex=False)
        
        # Ensure 'court' is the first column
        df = df[['court'] + [col for col in df.columns if col != 'court']]

    return df

def main(folder_path: str) -> pd.DataFrame:
    """
    Main function to read Excel files, combine them, and process the resulting DataFrame.

    Args:
        folder_path (str): Path to the folder containing Excel files.

    Returns:
        pd.DataFrame: Processed DataFrame containing data from all Excel files.
    """
    # Define column names
    column_names = [
        "line", "date_dd", "date_mon", "date_yyyy", "caseid_type", "caseid_no", "filed_dd",
        "filed_mon", "filed_yyyy", "original_court", "original_code", "original_number",
        "original_year", "case_type", "judge_1", "judge_2", "judge_3", "judge_4", "judge_5",
        "judge_6", "judge_7", "comingfor", "outcome", "reason_adj", "next_dd", "next_mon",
        "next_yyyy", "male_applicant", "female_applicant", "organization_applicant",
        "male_defendant", "female_defendant", "organization_defendant", "legalrep",
        "applicant_witness", "defendant_witness", "custody", "other_details"
    ]

    try:
        combined_df = read_excel_files(folder_path, column_names)
        
        # Example name_map and court_prefix_to_remove (you can modify these as needed)
        name_map = {'_High Court Div': '', '_High Court Civil': '', '_High Court Criminal': ''}
        court_prefix_to_remove = "High Court_High Court"
        
        processed_df = process_dataframe(combined_df, name_map, court_prefix_to_remove)
        logger.info("Data processing completed successfully.")
        return processed_df
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        raise

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("Usage: python script_name.py <folder_path>")
        sys.exit(1)

    folder_path = sys.argv[1]
    result_df = main(folder_path)
    print(result_df.head())
    print(f"Total rows: {len(result_df)}")