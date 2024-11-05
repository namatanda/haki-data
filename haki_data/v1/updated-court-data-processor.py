import argparse
import logging
import os
import pandas as pd
from multiprocessing import Pool
from typing import List, Optional, Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

COLUMN_NAMES = [
    "line", "date_dd", "date_mon", "date_yyyy", "caseid_type", "caseid_no", "filed_dd",
    "filed_mon", "filed_yyyy", "original_court", "original_code", "original_number",
    "original_year", "case_type", "judge_1", "judge_2", "judge_3", "judge_4", "judge_5",
    "judge_6", "judge_7", "comingfor", "outcome", "reason_adj", "next_dd", "next_mon",
    "next_yyyy", "male_applicant", "female_applicant", "organization_applicant",
    "male_defendant", "female_defendant", "organization_defendant", "legalrep",
    "applicant_witness", "defendant_witness", "custody", "other_details"
]

def generate_file_paths(root_folder: str, is_api_data: bool, start_year: Optional[int] = None, end_year: Optional[int] = None) -> List[str]:
    file_paths = []
    if is_api_data:
        file_paths = [os.path.join(root_folder, f) for f in os.listdir(root_folder) if f.endswith((".xls", ".xlsx"))]
    else:
        for root, _, files in os.walk(root_folder):
            for file in files:
                if file.endswith(".xlsx"):
                    file_path = os.path.join(root, file)
                    try:
                        year = int(os.path.basename(os.path.dirname(os.path.dirname(file_path))))
                        if start_year <= year <= end_year:
                            file_paths.append(file_path)
                    except ValueError:
                        continue
    return file_paths

def process_file(file_path: str, is_api_data: bool) -> pd.DataFrame:
    try:
        if is_api_data:
            court_name = os.path.basename(file_path).split("-")[0]
            df = pd.read_excel(file_path, header=4, names=COLUMN_NAMES)
        else:
            path_components = os.path.normpath(file_path).split(os.sep)
            court_name = path_components[-4]
            df = pd.read_excel(file_path, header=4, names=COLUMN_NAMES)
        
        df = df.assign(court_name=court_name).drop(df.columns[0], axis=1)
        return df
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return pd.DataFrame()

def process_files(file_paths: List[str], is_api_data: bool) -> pd.DataFrame:
    with Pool() as pool:
        results = pool.starmap(process_file, [(path, is_api_data) for path in file_paths])
    
    return pd.concat([df for df in results if not df.empty], ignore_index=True)

def process_dataframe(df: pd.DataFrame, is_api_data: bool, name_map: Optional[Dict[str, str]] = None, court_prefix_to_remove: Optional[str] = None) -> pd.DataFrame:
    float_columns = df.select_dtypes(include=['float64']).columns
    df[float_columns] = df[float_columns].fillna(0).astype(int)

    if is_api_data and (name_map or court_prefix_to_remove):
        def map_names(name: str) -> str:
            if name_map:
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
        df = df[['court'] + [col for col in df.columns if col != 'court']]

    return df

def main(args: argparse.Namespace) -> None:
    try:
        file_paths = generate_file_paths(args.root_folder, args.is_api_data, args.start_year, args.end_year)
        if not file_paths:
            raise ValueError("No Excel files found in the specified folder or year range.")

        combined_df = process_files(file_paths, args.is_api_data)
        
        name_map = {'_High Court Div': '', '_High Court Civil': '', '_High Court Criminal': ''} if args.use_name_map else None
        court_prefix_to_remove = "High Court_High Court" if args.remove_court_prefix else None
        
        processed_df = process_dataframe(combined_df, args.is_api_data, name_map, court_prefix_to_remove)
        
        processed_df.to_csv(args.output_file, index=False)
        logger.info(f"Processed data saved to {args.output_file}")
        logger.info(f"Total rows processed: {len(processed_df)}")
    except Exception as e:
        logger.error(f"An error occurred during data processing: {e}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process court data from Excel files.")
    parser.add_argument('root_folder', type=str, help='Root folder containing Excel files')
    parser.add_argument('output_file', type=str, help='Output CSV file path')
    parser.add_argument('--is_api_data', action='store_true', help='Flag to indicate if data is from API (flat structure)')
    parser.add_argument('--start_year', type=int, help='Start year for file processing (required for non-API data)')
    parser.add_argument('--end_year', type=int, help='End year for file processing (required for non-API data)')
    parser.add_argument('--use_name_map', action='store_true', help='Use name mapping for API data')
    parser.add_argument('--remove_court_prefix', action='store_true', help='Remove court prefix for API data')

    args = parser.parse_args()

    if not args.is_api_data and (args.start_year is None or args.end_year is None):
        parser.error("--start_year and --end_year are required for non-API data processing")

    main(args)
