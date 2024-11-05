import os
import pandas as pd
import argparse
import multiprocessing
from multiprocessing.pool import ApplyResult
import logging
from typing import List, Optional, Dict

# Define column names for Excel files
column_name: List[str] = [
    "line", "date_dd", "date_mon", "date_yyyy", "caseid_type", "caseid_no", "filed_dd",
    "filed_mon", "filed_yyyy", "original_court", "original_code", "original_number",
    "original_year", "case_type", "judge_1", "judge_2", "judge_3", "judge_4", "judge_5",
    "judge_6", "judge_7", "comingfor", "outcome", "reason_adj", "next_dd", "next_mon",
    "next_yyyy", "male_applicant", "female_applicant", "organization_applicant",
    "male_defendant", "female_defendant", "organization_defendant", "legalrep",
    "applicant_witness", "defendant_witness", "custody", "other_details"
]

def generate_file_paths(root_folder: str, start_year: Optional[int] = None, end_year: Optional[int] = None, recursive: bool = True) -> List[str]:
    """
    Generates a list of file paths for Excel files. Optionally filters by years if recursive is True.
    """
    logging.info(f"Generating file paths from {root_folder}, recursive={recursive}")
    file_paths: List[str] = []
    
    if recursive:
        if start_year is None or end_year is None:
            raise ValueError("start_year and end_year must be provided when recursive is True.")
        
        for root, _, files in os.walk(root_folder):
            for file in files:
                if file.endswith(".xlsx"):
                    file_path: str = os.path.join(root, file)
                    try:
                        year: int = int(os.path.basename(os.path.dirname(root)))
                        if start_year <= year <= end_year:
                            file_paths.append(file_path)
                            logging.info(f"Processed file path: {file_path}")
                    except ValueError as ve:
                        logging.error(f"Error processing file path {file_path}: {ve}")
    else:
        for file in os.listdir(root_folder):
            if file.endswith(".xlsx"):
                file_path: str = os.path.join(root_folder, file)
                file_paths.append(file_path)

    logging.info(f"Generated {len(file_paths)} file paths")
    return file_paths

def process_file(file_path: str) -> pd.DataFrame:
    """
    Processes a single Excel file, extracting the court code and relevant data.
    """
    logging.info(f"Entering process_file for file {file_path}")
    try: 
        path_components: List[str] = os.path.normpath(file_path).split(os.sep) 
        court_name: str = path_components[-4] if len(path_components) >= 4 else "Unknown Court"
        df: pd.DataFrame = pd.read_excel(file_path, header=4, names=column_name)  
        df = df.assign(court_name=court_name).drop(df.columns[0], axis=1)
        logging.info(f"Processed file: {file_path}, number of rows: {len(df)}")
        return df
    except (ValueError, pd.errors.EmptyDataError, pd.errors.ParserError) as e:
        logging.error(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()

def process_files(file_paths: List[str]) -> pd.DataFrame:
    """
    Reads and processes Excel files from a list of paths using multiple processes,
    combining them into a DataFrame, and logs file processing information.
    """
    logging.info("Entering process_files")
    
    with multiprocessing.Pool() as pool:
        results: List[ApplyResult] = [pool.apply_async(process_file, args=(path,)) for path in file_paths]
        processed_dfs: List[pd.DataFrame] = [result.get(timeout=60) for result in results if isinstance(result, ApplyResult)]

    combined_df: pd.DataFrame = pd.concat([df for df in processed_dfs if not df.empty], ignore_index=True)
    logging.info("Exiting process_files successfully")
    return combined_df 

def process_dataframe(df: pd.DataFrame, name_map: Optional[Dict[str, str]] = None, court_prefix_to_remove: Optional[str] = None) -> pd.DataFrame:
    """
    Process the combined DataFrame by cleaning court names and reordering columns.
    """
    # Fill NaN values in float columns with 0 and convert to int
    float_columns = df.select_dtypes(include=['float64']).columns
    df[float_columns] = df[float_columns].fillna(0).astype(int)

    if name_map:
        def map_names(name: str) -> str:
            for key, value in name_map.items():
                name = name.replace(key, value)
            return name.split()[0]

        df['court'] = df['court_name'].apply(map_names)
        
        if court_prefix_to_remove:
            df['court'] = df['court'].str.replace(court_prefix_to_remove, "", case=False, regex=False)
        
        df['court'] = df['court'].str.replace(r'\s+', ' ', regex=True)
        df = df[['court'] + [col for col in df.columns if col != 'court' and col != 'court_name']]
    else:
        df = df.rename(columns={'court_name': 'court'})
        if court_prefix_to_remove:
            df['court'] = df['court'].str.replace(court_prefix_to_remove, "", case=False, regex=False)
        df = df[['court'] + [col for col in df.columns if col != 'court']]

    return df

def save_to_csv(data_frame: pd.DataFrame, output_file: str) -> None:
    """
    Saves the processed data frame to a CSV file.
    """
    logging.info(f"Saving data to {output_file}")
    try:
        data_frame.to_csv(output_file, index=False)
        logging.info(f"Processed data saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving data to CSV: {e}")

def main():
    """
    Main function to process Excel files and output processed data to a CSV.
    Supports both recursive and non-recursive modes.
    """
    parser = argparse.ArgumentParser(description="Process Excel files and output to CSV.")
    parser.add_argument('root_folder', type=str, help='Root folder containing Excel files')
    parser.add_argument('output_file', type=str, help='Output CSV file path')
    parser.add_argument('--start_year', type=int, help='Start year for file processing (optional, used in recursive mode)')
    parser.add_argument('--end_year', type=int, help='End year for file processing (optional, used in recursive mode)')
    parser.add_argument('--recursive', action='store_true', help='Enable recursive search based on year folders')
    
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    try:
        # Generate file paths
        file_paths: List[str] = generate_file_paths(args.root_folder, args.start_year, args.end_year, args.recursive)
        
        # Process the files into a combined DataFrame
        combined_df = process_files(file_paths)
        
        # Process the DataFrame (modify name_map and court_prefix_to_remove as needed)
        name_map = {'_High Court Div': '', '_High Court Civil': '', '_High Court Criminal': ''}
        court_prefix_to_remove = "High Court_High Court"
        processed_df = process_dataframe(combined_df, name_map, court_prefix_to_remove)
        
        # Save the final processed DataFrame to CSV
        save_to_csv(processed_df, args.output_file)
        logging.info("Data processing and saving completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during execution: {e}")

if __name__ == "__main__":
    main()

#
# To run dcrt-template : python script_name.py <root_folder> <output_file> --start_year 2020 --end_year 2023 --recursive
# To run for api-data: python script_name.py <root_folder> <output_file>
#TODO : add capability to notify each file that has been successfully processed
#TODO : warn the user to enter the start year and end year correctly and set the recursive flag
#TODO : provide a summmary of the data processed after the script is executed
#TODO : add a log file to track the progress of the script
#TODO : provide feedback the script is completed successfully
