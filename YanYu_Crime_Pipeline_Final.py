"""
Data Pipeline for Crime, House Price, and Stop-and-Search Analysis

Overview:
-----------
This Python-based data pipeline is designed to automate the process of data ingestion, transformation, and reporting 
for datasets related to crime statistics, house prices, and stop-and-search events. The pipeline processes large 
amounts of raw data and transforms it into meaningful insights, which are then saved as structured CSV files ready 
for reporting and analysis. The pipeline is built to handle data across multiple regions, ensuring data quality 
throughout the ETL (Extract, Transform, Load) process.

Objective:
-----------
The main objective of this pipeline is to provide accurate and well-structured datasets for analyzing crime patterns, 
house price trends, and stop-and-search data across various regions. The pipeline includes data normalization steps 
and merges various data sources, enabling stakeholders to:
    - Identify crime trends across regions and over time.
    - Analyze correlations between crime and house prices at the district level.
    - Understand stop-and-search statistics broken down by demographic groups.
    - Create visualizations and reports for decision-making and policy implementation.

Data Sources:
--------------
The pipeline handles the following datasets:
    1. Crime Data: Includes details of crimes such as type, location, and outcome. Includes street, outcomes and stop-and-search
    Crime data can be downloaded from: https://data.police.uk/data/
    2. House Price Data: Contains transaction-level data for house prices across multiple regions.
    House price data can be downloaded from: https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads
    Postcode data can be downloaded from: https://www.doogal.co.uk/UKPostcodes
    3. Summary Statistics: Regional population, area, and budget data used for calculating normalized metrics like 
       crime per capita and crime per budget. This data was found on each police force wikipedia. 

Make a folder called 'EastofEngland_Dataset' within this folder there should be:
    - 'Crime_Dataset' folder which contains all the crime data folders saved from the website
    (These folders will be saved by year and month i.e., [YYYY]-[MM])
    - 'House_Price_Dataset' folder with the csvs for relevant years
    - 'postcodes.csv' file
    - 'summary_stats.csv' file

Pipeline Layers:
-----------------
1. Staging Layer:
    - This layer ingests and cleans raw data by removing duplicates and invalid entries (e.g., invalid age ranges).
    - It applies basic transformations and prepares the data for further processing in the primary layer.

2. Primary Layer:
    - This layer merges and aggregates the staged data, performing more complex transformations to combine data 
      from different sources (e.g., merging crime data with regional postcodes and stop-and-search data with demographic info).
    - Key transformations also include applying region-based aggregations and normalizing crime statistics by population and budget.

3. Reporting Layer:
    - The final step of the pipeline, where the transformed data is aggregated further for visualizations and reporting.
    - Region-based summaries, crime trends over time, and demographic breakdowns for stop-and-search events are 
      generated and saved as CSV files.
    - Metrics such as crime per 1000 people, crime per £1M budget, and average house price are calculated and included in the summary tables.

How to Use:
-----------
1. Set the base path to the directory where your raw datasets are stored.
2. Ensure that the raw data files are stored in their respective folders (e.g., 'staged_data', 'primary_layer_data').
3. Run each layer of the pipeline in sequence:
    - The Staging Layer for raw data ingestion and cleaning.
    - The Primary Layer for data transformation and merging.
    - The Reporting Layer for creating summary tables and saving final reports.

Error Handling:
---------------
Each step of the pipeline includes error handling and logging to ensure that the data processing is tracked, and any 
issues can be easily identified. The pipeline will raise appropriate errors for missing data files, inconsistent data, 
or other issues encountered during execution.
"""


#Packages to Import
import os
import pandas as pd
import logging
from collections import defaultdict
import numpy as np
import re
from scipy.spatial import KDTree


# Configure logging
logging.basicConfig(
    filename='pipeline.log',  # Log to a file
    level=logging.INFO,       # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
    datefmt='%Y-%m-%d %H:%M:%S'
)


#Functions used in pipeline:
def import_crime_data(base_path, police_forces):
    """
    Imports crime CSV data files from a specified base directory and for a given list of police forces.

    Parameters:
        base_path (str): The base directory containing the 'Crime_Dataset' folder.
        police_forces (list): A list of police forces to include (e.g., 'norfolk', 'suffolk').

    Returns:
        dict: A dictionary with keys representing data types and values as DataFrames.
    """
    crime_data = {}
    dataset_dir = os.path.join(base_path, 'Crime_Dataset')  # Adjust base path to include 'Crime_Dataset'
    logging.info(f'Starting data import from directory: {dataset_dir}')

    # Check if the dataset directory exists
    if not os.path.exists(dataset_dir):
        logging.error(f"Dataset directory does not exist: {dataset_dir}")
        return crime_data

    for folder in sorted(os.listdir(dataset_dir)):
        folder_path = os.path.join(dataset_dir, folder)

        # Skip if the folder is not a valid directory
        if not os.path.isdir(folder_path):
            logging.info(f"Skipping invalid folder: {folder}")
            continue

        logging.info(f'Processing folder: {folder}')
        successful_imports = []

        for file in sorted(os.listdir(folder_path)):
            if file.endswith('.csv'):
                file_path = os.path.join(folder_path, file)

                # Ensure the file follows the correct naming convention
                try:
                    parts = file[:-4].split('-')
                    if len(parts) < 4:  # Invalid file name structure
                        logging.warning(f"Skipping incorrectly named file: {file}")
                        continue

                    year_month = parts[0] + '-' + parts[1]
                    force = parts[2]
                    data_type = '-'.join(parts[3:])

                    if force in police_forces:
                        key = f'{year_month}-{force}-{data_type}'
                        try:
                            crime_data[key] = pd.read_csv(file_path)
                            successful_imports.append(f"{file} as {key}")
                        except Exception as e:
                            logging.error(f'Failed to load file {file}: {e}')
                            continue  # Continue with the next file if an error occurs
                except Exception as e:
                    logging.error(f'Error processing file name {file}: {e}')
                    continue  # Continue with the next file

        if successful_imports:
            logging.info(f'Successfully imported files from folder {folder}:\n' + '\n'.join(successful_imports))
        else:
            logging.info(f'No files were imported from folder {folder}')

    logging.info('Crime data import completed.')
    return crime_data

def load_house_price_data(base_path):
    """
    Load house price data from all CSV files in the House_Price_Dataset directory.

    Parameters:
        base_path (str): Path to the directory where the house price CSV files are located.

    Returns:
        dict: A dictionary where keys are file names and values are corresponding DataFrames.
    """
    house_price_data = {}
    dataset_dir = os.path.join(base_path, 'House_Price_Dataset')
    
    logging.info(f"Loading house price data from: {dataset_dir}")
    
    if not os.path.exists(dataset_dir):
        logging.error(f"House price dataset directory does not exist: {dataset_dir}")
        return house_price_data
    
    files = [f for f in os.listdir(dataset_dir) if f.endswith('.csv')]
    
    if not files:
        logging.warning(f"No CSV files found in the house price dataset directory: {dataset_dir}")
    
    for file in files:
        file_path = os.path.join(dataset_dir, file)
        logging.info(f"Attempting to load house price file: {file}")
        
        try:
            df = pd.read_csv(file_path)
            if file == "pp-monthly-update-new-version.csv":
                key = "recent"
            else:
                key = file.replace("pp-", "").replace(".csv", "")
            house_price_data[key] = df
            logging.info(f"Successfully loaded {file} as {key}")
        except Exception as e:
            logging.error(f"Failed to load {file}: {e}")
    
    return house_price_data

def combine_dataframes(crime_data):
    """
    This function combines data frames by concatenating them based on police force and data type.
    
    Parameters:
    dataframes (dict): A dictionary of data frames where the key is a string containing
                        year-month-force-data_type information.
    
    Returns:
    combined_crime_df (dict): A dictionary with combined data frames for each police force and data type.
    """
    combined_crime_df = {}  # Dictionary to store combined data frames for each police force and data type
    logging.info('Starting to combine dataframes.')

    for key, df in crime_data.items():
        parts = key.split('-')
        force = '-'.join(parts[2:-1])  # Extract police force
        data_type = parts[-1]  # Extract data type
        composite_key = f'{force}-{data_type}'  # Create a composite key from the force and type

        try:
            if composite_key in combined_crime_df:
                # If the composite key already exists, concatenate the new DataFrame to it
                combined_crime_df[composite_key] = pd.concat([combined_crime_df[composite_key], df], ignore_index=True)
            else:
                # Otherwise, start a new DataFrame for this composite key
                combined_crime_df[composite_key] = df
        except Exception as e:
            logging.error(f'Error combining data for key {key}: {e}')  # Log the error
            continue

    logging.info('Finished combining dataframes.')
    return combined_crime_df

def check_and_drop_empty_columns(combined_crime_df):
    """
    Checks for and drops columns that are entirely NaN or empty in each DataFrame.
    
    Parameters:
    combined_crime_df (dict): A dictionary where keys are identifiers and values are Pandas DataFrames.
    
    Returns:
    combined_crime_df (dict): Updated dictionary with empty columns removed.
    """
    logging.info('Starting check and drop of empty columns.')

    for key, df in combined_crime_df.items():
        # Check if the value is a DataFrame
        if isinstance(df, pd.DataFrame):
            # Find columns that are entirely NaN
            empty_columns = df.columns[df.isna().all()].tolist()
            
            if empty_columns:
                logging.info(f"DataFrame '{key}' has completely empty columns: {', '.join(empty_columns)}")
                # Drop empty columns
                combined_crime_df[key] = df.drop(empty_columns, axis=1)
                logging.info(f"Dropped empty columns from DataFrame '{key}'")
            else:
                logging.info(f"DataFrame '{key}' has no completely empty columns.")
        else:
            logging.error(f"Expected a DataFrame for key '{key}', but got {type(df).__name__} instead.")

    logging.info('Finished check and drop of empty columns.')
    return combined_crime_df


def remove_duplicates(combined_crime_df):
    """
    This function removes duplicate rows from each DataFrame in a dictionary.
    It logs the number of rows removed and the percentage of data removed for each DataFrame.
    
    Parameters:
    combined_crime_df (dict): A dictionary where the keys are identifiers (e.g., police force and data type),
                          and the values are Pandas DataFrames.
    
    Returns:
    combined_crime_df (dict): Updated dictionary with duplicates removed from each DataFrame.
    """
    logging.info('Starting to remove duplicate rows from dataframes.')
    
    for key, df in combined_crime_df.items():
        # Check if the value is a DataFrame
        if isinstance(df, pd.DataFrame):
            initial_count = df.shape[0]  # Initial number of rows
            df_clean = df.drop_duplicates(keep='first')  # Remove duplicates, keep first occurrence
            final_count = df_clean.shape[0]  # Final number of rows after removing duplicates
            removed = initial_count - final_count  # Number of rows removed
            percentage_removed = (removed / initial_count) * 100  # Percentage of rows removed
            
            # Replace the original DataFrame with the cleaned one
            combined_crime_df[key] = df_clean

            # Log the results
            logging.info(f"{key}: Rows after duplicates removed = {final_count}, "
                         f"Rows removed = {removed}, Percentage removed = {percentage_removed:.2f}%")
        else:
            logging.error(f"Expected a DataFrame for key '{key}', but got {type(df).__name__} instead.")
        
    logging.info('Finished removing duplicate rows from all dataframes.')
    return combined_crime_df


def clean_text_columns(combined_crime_df):
    """
    Standardizes specified columns by converting the text to lowercase and stripping leading/trailing spaces.

    Parameters:
    combined_crime_df (dict): A dictionary where keys are region-data_type and values are DataFrames.

    Returns:
    combined_crime_df (dict): The updated dictionary with standardized text columns.
    """
    logging.info('Starting to clean text columns in DataFrames.')

    for key, df in combined_crime_df.items():
        logging.info(f'Processing DataFrame: {key}')
        
        # Standardize 'Location' column
        if 'Location' in df.columns:
            df['Location'] = df['Location'].str.lower().str.strip()
            logging.info(f"'Location' column standardized in {key}.")

        # Standardize 'Crime type' column
        if 'Crime type' in df.columns:
            df['Crime type'] = df['Crime type'].str.lower().str.strip()
            logging.info(f"'Crime type' column standardized in {key}.")

        # Standardize 'Outcome type' column
        if 'Outcome type' in df.columns:
            df['Outcome type'] = df['Outcome type'].str.lower().str.strip()
            logging.info(f"'Outcome type' column standardized in {key}.")

        # Standardize 'Outcome' column
        if 'Outcome' in df.columns:
            df['Outcome'] = df['Outcome'].str.lower().str.strip()
            logging.info(f"'Outcome' column standardized in {key}.")

        # Standardize 'Last outcome category' column
        if 'Last outcome category' in df.columns:
            df['Last outcome category'] = df['Last outcome category'].str.lower().str.strip()
            logging.info(f"'Last outcome category' column standardized in {key}.")

        # Standardize 'Object of search' column
        if 'Object of search' in df.columns:
            df['Object of search'] = df['Object of search'].astype(str).str.lower().str.strip()
            logging.info(f"'Object of search' column standardized in {key}.")

        # Update the dictionary with the modified DataFrame
        combined_crime_df[key] = df

    logging.info('Finished cleaning text columns in all DataFrames.')
    return combined_crime_df


def split_date_columns(combined_crime_df):
    """
    Splits 'Month' and 'Year' for 'street' and 'outcomes' data types.
    Splits 'Day', 'Month', and 'Year' for 'stop-and-search' data types.

    Parameters:
    combined_crime_df (dict): A dictionary where the keys are identifiers (e.g., 'region-data_type') and values are DataFrames.

    Returns:
    combined_crime_df (dict): The updated dictionary with split date columns for each relevant DataFrame.
    """
    logging.info('Starting to split date columns for DataFrames.')

    # Iterate through the dictionary to process 'street' and 'outcomes' DataFrames
    for key, df in combined_crime_df.items():
        if 'street' in key or 'outcomes' in key:
            if 'Month' in df.columns:
                logging.info(f"Processing 'Month' column in DataFrame: {key}")
                # Split the 'Month' column into 'Year' and 'Month'
                df['Year'], df['Month'] = df['Month'].str.split('-', expand=True)[0], df['Month'].str.split('-', expand=True)[1]
                combined_crime_df[key] = df

    # Iterate through the dictionary to process 'stop-and-search' DataFrames
    for key, df in combined_crime_df.items():
        if 'stop-and-search' in key:
            if 'Date' in df.columns:
                logging.info(f"Processing 'Date' column in stop-and-search DataFrame: {key}")
                # Convert 'Date' column to datetime format
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce') 
                df['Year'] = df['Date'].dt.year
                df['Month'] = df['Date'].dt.month
                df['Day'] = df['Date'].dt.day
                # Drop the 'Date' column after extracting the necessary components
                df = df.drop('Date', axis=1)
                combined_crime_df[key] = df

    logging.info('Finished splitting date columns for all DataFrames.')
    return combined_crime_df


def remove_rows_with_missing_data(combined_crime_df, columns_to_check=['Latitude', 'Longitude', 'LSOA code', 'LSOA name']):
    """
    Removes rows where any of the specified columns contain NaN values and logs the number of rows removed.

    Parameters:
    combined_crime_df (dict): A dictionary where the keys are region-data_type and values are DataFrames.
    columns_to_check (list): List of columns to check for NaN values. Rows with NaNs in these columns will be removed.

    Returns:
    combined_crime_df (dict): The updated dictionary with rows containing NaN values in the specified columns removed.
    """
    logging.info('Starting row removal based on missing values in specified columns.')

    for key, df in combined_crime_df.items():
        # Check which columns exist in the current DataFrame
        existing_columns = [col for col in columns_to_check if col in df.columns]

        if existing_columns:
            total_rows_before = df.shape[0]  # Number of rows before removal
            df_cleaned = df.dropna(subset=existing_columns)  # Remove rows with NaN in any of the specified columns
            total_rows_after = df_cleaned.shape[0]  # Number of rows after removal
            combined_crime_df[key] = df_cleaned  # Update the DataFrame in the dictionary

            # Calculate the number of rows removed and the percentage of rows deleted
            rows_removed = total_rows_before - total_rows_after
            if total_rows_before > 0:
                percentage_deleted = (rows_removed / total_rows_before) * 100
                logging.info(f"In DataFrame '{key}', {rows_removed} rows were deleted, which is {percentage_deleted:.2f}% of the total.")
            else:
                logging.info(f"DataFrame '{key}' was initially empty or all rows were removed.")
        else:
            logging.warning(f"No specified columns found in DataFrame '{key}'. Skipping row removal.")

    logging.info('Finished row removal based on missing values in specified columns.')
    return combined_crime_df


def replace_nans_with_unknown(data, columns_to_replace):
    """
    Replaces NaN values and empty strings in specified columns with 'Unknown' and logs the replacement counts.

    Parameters:
    data (dict or pd.DataFrame): If dict, the keys are region-data_type and values are DataFrames (for crime data).
                                 If pd.DataFrame, it's the house price data.
    columns_to_replace (list): List of columns to replace NaN values and empty strings with 'Unknown'.

    Returns:
    data (dict or pd.DataFrame): The updated data with NaN and empty values replaced in the specified columns.
    """
    logging.info('Starting to replace NaN and empty values with "Unknown" in specified columns.')

    if isinstance(data, dict):
        # Handle dictionary case (crime data)
        for key, df in data.items():
            replacement_counts = {}  # Dictionary to store counts of replaced NaNs and empty strings for each column
            for column in columns_to_replace:
                if column in df.columns:
                    # Count NaN and empty string values
                    nan_count = df[column].isna().sum()
                    empty_count = (df[column] == '').sum()

                    # Replace NaNs and empty strings with 'Unknown'
                    if nan_count > 0 or empty_count > 0:
                        df[column] = df[column].replace('', 'Unknown').fillna('Unknown')
                        replacement_counts[column] = nan_count + empty_count  # Store count of replacements

            data[key] = df  # Update the DataFrame in the dictionary

            if replacement_counts:
                replacement_details = ', '.join([f'{col}: {count}' for col, count in replacement_counts.items()])
                logging.info(f"In DataFrame '{key}', replaced NaN and empty values with 'Unknown' in the following columns and counts: {replacement_details}")
            else:
                logging.info(f"DataFrame '{key}' had no NaN or empty values to replace.")

    elif isinstance(data, pd.DataFrame):
        # Handle single DataFrame case (house price data)
        replacement_counts = {}  # Dictionary to store counts of replaced NaNs and empty strings for each column
        for column in columns_to_replace:
            if column in data.columns:
                # Count NaN and empty string values
                nan_count = data[column].isna().sum()
                empty_count = (data[column] == '').sum()

                # Replace NaNs and empty strings with 'Unknown'
                if nan_count > 0 or empty_count > 0:
                    data[column] = data[column].replace('', 'Unknown').fillna('Unknown')
                    replacement_counts[column] = nan_count + empty_count  # Store count of replacements

        if replacement_counts:
            replacement_details = ', '.join([f'{col}: {count}' for col, count in replacement_counts.items()])
            logging.info(f"Replaced NaN and empty values with 'Unknown' in the following columns for house price data: {replacement_details}")
        else:
            logging.info("House price data had no NaN or empty values to replace.")

    logging.info('Finished replacing NaN and empty values with "Unknown".')
    return data


def stage_postcode_data(base_path):
    """
    Staging Layer: Processes postcode data by selecting relevant columns 
    and filtering based on the LSOA codes and active status.

    Parameters:
        postcodes (pd.DataFrame): The pre-imported postcode DataFrame.

    Returns:
        pd.DataFrame: Filtered and relevant postcode data for further processing.
    """
    
    postcode_file = os.path.join(base_path, 'postcodes.csv')
    postcodes = pd.read_csv(postcode_file)
    logging.info(f"Initial shape of postcode data: {postcodes.shape}")
    
    # Select relevant columns for processing
    relevant_columns = ['Postcode', 'In Use?', 'Latitude', 'Longitude', 'District']
    postcodes_selected = postcodes[relevant_columns]
    
    # Filter to only include in-use postcodes
    mask = postcodes_selected['In Use?'] == 'Yes'
    staged_postcodes = postcodes_selected[mask]
    
    logging.info(f"Filtered postcode data: {staged_postcodes.shape}")
    
    # Log unique values in the 'In Use?' column
    logging.info(f"Unique values in the 'In Use?' column: {postcodes['In Use?'].value_counts().to_dict()}")
    
    return staged_postcodes


def apply_house_price_column_headers(house_price_data):
    """
    Applies column headers to each house price DataFrame in the house_price_data dictionary based on the provided structure from the dataset source.
    
    Parameters:
        house_price_data (dict): Dictionary where keys are years/identifiers and values are house price DataFrames without headers.
    
    Returns:
        dict: A dictionary with the same keys, where each DataFrame has the applied headers.
    """
    logging.info("Applying column headers to the house price datasets.")
    
    # List of column headers based on the dataset source
    column_headers = [
        'Transaction unique identifier', 'Price', 'Date of Transfer', 'Postcode', 'Property Type', 
        'Old/New', 'Duration', 'PAON', 'SAON', 'Street', 'Locality', 'Town/City', 'District', 
        'County', 'PPD Category Type', 'Record Status'
    ]
    
    # Iterate over the house price data dictionary
    for key, df in house_price_data.items():
        # Ensure the number of columns matches the header length
        if len(df.columns) != len(column_headers):
            logging.error(f"DataFrame for {key} has {len(df.columns)} columns, but expected {len(column_headers)}.")
            raise ValueError(f"Column mismatch: DataFrame for {key} does not have the expected number of columns.")
        
        # Apply the column headers to the DataFrame
        df.columns = column_headers
        house_price_data[key] = df  # Update the dictionary with the DataFrame having the correct headers
        
        logging.info(f"Successfully applied {len(column_headers)} column headers to the house price dataset for {key}.")
    
    return house_price_data


def combine_house_price_data(house_price_data):
    """
    Combines house price data from multiple years into a single DataFrame.
    
    Parameters:
        house_price_data (dict): A dictionary where keys are years or identifiers (e.g., '2021', '2022', 'recent') 
                                 and values are DataFrames containing the house price data for that period.
    
    Returns:
        pd.DataFrame: A single combined DataFrame containing data from all years with an additional 'Year' column.
    """
    combined_price_df = pd.DataFrame()  # Initialize an empty DataFrame
    
    # Iterate over the house price data dictionary
    for key, df in house_price_data.items():
        year = key.replace('pp-', '')  # Removes 'pp-' prefix and leaves year or 'recent'
        if year == 'monthly-update-new-version':
            year = 'recent'
        df['Year'] = year
        
        # Append the current DataFrame to the combined DataFrame
        combined_price_df = pd.concat([combined_price_df, df], ignore_index=True)
        logging.info(f"Appended data from {key} with {len(df)} rows to the combined dataframe.")

    logging.info(f"Combined house price data into a single DataFrame with {len(combined_price_df)} rows.")
    
    return combined_price_df


def remove_duplicates_from_combined_price_df(combined_price_df):
    """
    Checks for and removes duplicate rows in the combined house price DataFrame, 
    both overall and based on the 'Transaction unique identifier' column, 
    and logs the percentage of duplicates removed.
    
    Parameters:
        combined_price_df (pd.DataFrame): The combined house price DataFrame with potential duplicates.
    
    Returns:
        pd.DataFrame: A DataFrame with duplicate rows removed.
    """
    logging.info("Checking for duplicate rows in the combined house price data.")
    
    # Total number of rows before removing any duplicates
    total_rows = len(combined_price_df)
    
    # 1. Check for overall duplicate rows
    overall_duplicate_count = combined_price_df.duplicated().sum()
    
    if overall_duplicate_count > 0:
        # Calculate percentage of overall duplicate rows
        percentage_deleted_overall = (overall_duplicate_count / total_rows) * 100
        logging.info(f"Found {overall_duplicate_count} duplicate rows in the house price data.")
        logging.info(f"Overall duplicates account for {percentage_deleted_overall:.2f}% of the total rows.")
        
        # Remove overall duplicate rows
        combined_price_df = combined_price_df.drop_duplicates()
        logging.info(f"Removed {overall_duplicate_count} overall duplicate rows from the house price data.")
    else:
        logging.info("No overall duplicate rows found in the house price data.")
    
    # 2. Check for duplicates in 'Transaction unique identifier' column
    if 'Transaction unique identifier' in combined_price_df.columns:
        transaction_duplicate_count = combined_price_df.duplicated(subset=['Transaction unique identifier']).sum()
        
        if transaction_duplicate_count > 0:
            # Calculate percentage of duplicates based on 'Transaction unique identifier'
            percentage_deleted_transaction = (transaction_duplicate_count / total_rows) * 100
            logging.info(f"Found {transaction_duplicate_count} duplicate rows based on 'Transaction unique identifier'.")
            logging.info(f"Transaction ID duplicates account for {percentage_deleted_transaction:.2f}% of the total rows.")
            
            # Remove duplicates based on 'Transaction unique identifier'
            combined_price_df = combined_price_df.drop_duplicates(subset=['Transaction unique identifier'])
            logging.info(f"Removed {transaction_duplicate_count} duplicate rows based on 'Transaction unique identifier'.")
        else:
            logging.info("No duplicate 'Transaction unique identifier' found.")
    else:
        logging.error("'Transaction unique identifier' column not found in the house price data.")
    
    # Return the cleaned DataFrame
    return combined_price_df


def clean_and_split_date_columns(combined_price_df):
    """
    Drops the 'Year' column and splits the 'Date of Transfer' column into separate 'Day', 'Month', and 'Year' columns,
    while handling the time component present in the 'Date of Transfer'.
    
    Parameters:
        combined_price_df (pd.DataFrame): The DataFrame containing house price data with a 'Date of Transfer' column.
    
    Returns:
        pd.DataFrame: The modified DataFrame with separate 'Day', 'Month', and 'Year' columns.
    """
    # Step 1: Drop the 'Year' column if it exists
    if 'Year' in combined_price_df.columns:
        logging.info("Dropping the 'Year' column.")
        combined_price_df = combined_price_df.drop(columns=['Year'])

    # Step 2: Ensure 'Date of Transfer' column exists
    if 'Date of Transfer' in combined_price_df.columns:
        logging.info("Splitting 'Date of Transfer' into 'Day', 'Month', and 'Year'.")

        # Step 3: Convert 'Date of Transfer' to datetime format, focusing only on the date (ignore time)
        combined_price_df['Date of Transfer'] = pd.to_datetime(combined_price_df['Date of Transfer']).dt.date

        # Step 4: Create new columns for 'Day', 'Month', and 'Year'
        combined_price_df['Day'] = pd.to_datetime(combined_price_df['Date of Transfer']).dt.day
        combined_price_df['Month'] = pd.to_datetime(combined_price_df['Date of Transfer']).dt.month
        combined_price_df['Year'] = pd.to_datetime(combined_price_df['Date of Transfer']).dt.year

        # Drop the original 'Date of Transfer' column if necessary
        combined_price_df = combined_price_df.drop(columns=['Date of Transfer'])
    else:
        logging.warning("'Date of Transfer' column not found. Skipping date split.")

    return combined_price_df


def drop_unnecessary_columns(data, columns_to_drop):
    """
    Drops unnecessary columns from either a single DataFrame or a dictionary of DataFrames.

    Parameters:
        data (pd.DataFrame or dict): The data to process. Can be a single DataFrame or a dictionary of DataFrames.
        columns_to_drop (list): A list of columns to drop from the DataFrame(s).

    Returns:
        pd.DataFrame or dict: The processed DataFrame or dictionary of DataFrames with the unnecessary columns removed.
    """
    # If the input is a single DataFrame
    if isinstance(data, pd.DataFrame):
        logging.info(f"Dropping unnecessary columns from a single DataFrame: {', '.join(columns_to_drop)}")
        data = data.drop(columns=columns_to_drop, errors='ignore')
    
    # If the input is a dictionary of DataFrames
    elif isinstance(data, dict):
        logging.info(f"Dropping unnecessary columns from multiple DataFrames.")
        for key, df in data.items():
            logging.info(f"Dropping columns from {key}: {', '.join(columns_to_drop)}")
            data[key] = df.drop(columns=columns_to_drop, errors='ignore')
    
    else:
        logging.warning("Input data is neither a DataFrame nor a dictionary of DataFrames. No changes made.")
    
    return data


def remove_rows_with_missing_values(combined_price_df, columns_to_check):
    """
    Removes rows from the house price data where any of the specified columns have missing (NaN) values.
    Logs the percentage of deleted entries.
    
    Parameters:
        combined_price_df (pd.DataFrame): The combined house price DataFrame.
        columns_to_check (list): List of columns to check for missing values (NaNs).
    
    Returns:
        pd.DataFrame: The DataFrame with rows removed where specified columns had NaN values.
    """
    # Get the initial number of rows
    initial_row_count = len(combined_price_df)
    
    # Drop rows where any of the specified columns have NaN values
    combined_price_df = combined_price_df.dropna(subset=columns_to_check)
    
    # Get the number of rows after removal
    final_row_count = len(combined_price_df)
    
    # Calculate the percentage of deleted rows
    deleted_rows = initial_row_count - final_row_count
    percentage_deleted = (deleted_rows / initial_row_count) * 100
    
    # Log the result
    logging.info(f"Deleted {deleted_rows} rows from house price data, which is {percentage_deleted:.2f}% of the total rows.")
    
    return combined_price_df


def replace_property_type_and_old_new_keys(combined_price_df):
    """
    Replaces keys in the 'Property Type' and 'Old/New' columns and removes rows with invalid values.
    Logs the number of rows removed.

    Parameters:
        combined_price_df (pd.DataFrame): The combined house price DataFrame.

    Returns:
        pd.DataFrame: The updated DataFrame with replaced keys and invalid rows removed.
    """
    # Define mappings for Property Type and Old/New
    property_type_mapping = {
        'D': 'Detached',
        'S': 'Semi-detached',
        'T': 'Terraced',
        'F': 'Flats / Maisonettes',
        'O': 'Other'
    }

    old_new_mapping = {
        'Y': 'Newly built property',
        'N': 'Established property'
    }

    # Log initial row count
    initial_row_count = len(combined_price_df)

    # Replace Property Type values
    combined_price_df['Property Type'] = combined_price_df['Property Type'].map(property_type_mapping)

    # Replace Old/New values
    combined_price_df['Old/New'] = combined_price_df['Old/New'].map(old_new_mapping)

    # Drop rows where Property Type or Old/New have NaN values (invalid values)
    combined_price_df = combined_price_df.dropna(subset=['Property Type', 'Old/New'])

    # Log the number of rows removed
    final_row_count = len(combined_price_df)
    deleted_rows = initial_row_count - final_row_count
    percentage_deleted = (deleted_rows / initial_row_count) * 100
    
    logging.info(f"Deleted {deleted_rows} rows with invalid 'Property Type' or 'Old/New' values, which is {percentage_deleted:.2f}% of the total rows.")
    
    return combined_price_df


def correct_data_types(df, expected_types, df_name):
    for column, expected_type in expected_types.items():
        if column in df.columns:
            actual_type = df[column].dtype
            if actual_type != expected_type:
                try:
                    logging.info(f"{df_name}: Converting column '{column}' from {actual_type} to {expected_type}.")
                    
                    if expected_type == str:
                        df.loc[:, column] = df[column].astype(str)
                    elif expected_type == int:
                        df.loc[:, column] = pd.to_numeric(df[column], errors='coerce').fillna(0).astype(int)  # Handle NaNs
                    elif expected_type == float:
                        df.loc[:, column] = pd.to_numeric(df[column], errors='coerce').astype(float)
                    elif expected_type == bool:
                        # If boolean values are stored as strings ('True', 'False'), map them to actual booleans
                        df.loc[:, column] = df[column].map({'True': True, 'False': False}).astype(bool)

                except Exception as e:
                    logging.error(f"Failed to convert column '{column}' in {df_name} from {actual_type} to {expected_type}: {e}")
            else:
                logging.info(f"{df_name}: Column '{column}' already has the correct type {expected_type}.")
        else:
            logging.warning(f"{df_name}: Column '{column}' not found in DataFrame.")
    return df


def validate_data_types(data, expected_types, dataset_name):
    """
    Validates that the data types of the columns in a DataFrame match the expected data types.

    Parameters:
    - data (pd.DataFrame): The DataFrame to validate.
    - expected_types (dict): A dictionary where keys are column names and values are expected data types.
    - dataset_name (str): The name of the dataset being validated (for logging purposes).
    
    Returns:
    - None: Logs any mismatches found between the actual and expected data types.
    """
    logging.info(f"Validating data types for {dataset_name}.")

    # Map Python data types to their corresponding NumPy types for consistency in comparison
    python_to_numpy_types = {
        int: [np.int64, np.int32],  # Handle both int32 and int64
        float: np.float64,
        str: object,  # Pandas uses 'object' for strings
        bool: np.bool_,
    }

    for column, expected_type in expected_types.items():
        if column in data.columns:
            actual_type = data[column].dtype

            # Normalize expected type (if it's a Python type, convert to corresponding NumPy type)
            expected_normalized_type = python_to_numpy_types.get(expected_type, expected_type)

            # If expected type is a list (e.g., int32 or int64), check if actual type matches any of the valid options
            if isinstance(expected_normalized_type, list):
                if actual_type not in expected_normalized_type:
                    logging.warning(
                        f"{dataset_name}: Column '{column}' has type {actual_type}, expected one of {expected_normalized_type}."
                    )
            else:
                # Check if the actual type matches the normalized expected type
                if not pd.api.types.is_dtype_equal(actual_type, expected_normalized_type):
                    logging.warning(
                        f"{dataset_name}: Column '{column}' has type {actual_type}, expected {expected_normalized_type}."
                    )
        else:
            logging.warning(f"{dataset_name}: Column '{column}' not found in the data.")

    logging.info(f"Data type validation completed for {dataset_name}.")


def load_staged_data(base_path, police_forces, data_types):
    """
    Load staged crime, house price, and postcode data from the 'staged_data' folder.

    Parameters:
        base_path (str): Base directory where the 'staged_data' folder is located.
        police_forces (list): List of police forces (regions) to load data for.
        data_types (list): List of data types to load (e.g., 'street', 'outcomes').

    Returns:
        dict: A dictionary (staged_df) containing the crime data DataFrames with keys as 'staged_[region]-[data_type]'.
        pd.DataFrame: A DataFrame (staged_price_df) containing the house price data.
        pd.DataFrame: A DataFrame (staged_postcodes) containing the staged postcode data.
    """
    staged_df = {}
    staged_data_dir = os.path.join(base_path, 'staged_data')  # Adjust base path to include 'staged_data'

    # Load the crime data into a dictionary
    for force in police_forces:
        for data_type in data_types:
            file_path = os.path.join(staged_data_dir, f'staged_{force}-{data_type}.csv')
            try:
                staged_df[f'staged_{force}-{data_type}'] = pd.read_csv(file_path)
                logging.info(f"Successfully loaded {file_path}")
            except FileNotFoundError as e:
                logging.error(f"Error loading {file_path}: {e}")

    # Load the staged house price data
    staged_price_file = os.path.join(staged_data_dir, 'staged_house_price_data.csv')
    try:
        staged_price_df = pd.read_csv(staged_price_file)
        logging.info(f"Successfully loaded {staged_price_file}")
    except FileNotFoundError as e:
        logging.error(f"Error loading {staged_price_file}: {e}")
        staged_price_df = pd.DataFrame()  # Empty DataFrame if file not found

    # Load the staged postcode data
    staged_postcode_file = os.path.join(staged_data_dir, 'staged_postcodes.csv')
    try:
        staged_postcodes = pd.read_csv(staged_postcode_file)
        logging.info(f"Successfully loaded {staged_postcode_file}")
    except FileNotFoundError as e:
        logging.error(f"Error loading {staged_postcode_file}: {e}")
        staged_postcodes = pd.DataFrame()  # Empty DataFrame if file not found

    # Return the staged crime data (dictionary), house price data (DataFrame), and postcode data (DataFrame)
    return staged_df, staged_price_df, staged_postcodes


def merge_street_outcomes(staged_df):
    """
    Simplified function to merge 'Outcome type' from 'outcomes' into 'street' DataFrame for each region.
    This function assumes that the DataFrames in `staged_df` are named like 'staged_[region]-street' and 
    'staged_[region]-outcomes'.
    
    Parameters:
    staged_df (dict): A dictionary of DataFrames where keys are 'staged_[region]-street' and 'staged_[region]-outcomes'.
    
    Returns:
    merged_crime_df (dict): A dictionary where keys are regions and values are merged DataFrames.
    """
    merged_crime_df = {}

    logging.info("Starting simplified merging of street and outcomes DataFrames.")
    
    for key, df in staged_df.items():
        # Check if the key contains 'street'
        if 'street' in key:
            try:
                # Extract the region from the key using more flexible logic
                region = key.replace('staged_', '').replace('-street', '')

                # Create the corresponding outcomes key (e.g., 'staged_norfolk-outcomes')
                outcomes_key = f"staged_{region}-outcomes"
                
                # Ensure the outcomes DataFrame exists
                if outcomes_key in staged_df:
                    outcomes_df = staged_df[outcomes_key]
                    
                    try:
                        # Filter out rows where 'Crime ID' is 'unknown' in both street and outcomes DataFrames
                        df_filtered = df[df['Crime ID'] != 'unknown']
                        outcomes_filtered = outcomes_df[outcomes_df['Crime ID'] != 'Unknown']
                        
                        # Merge 'Outcome type' column from outcomes to street
                        merged_crime_df[region] = pd.merge(
                            df_filtered, 
                            outcomes_filtered[['Crime ID', 'Outcome type']],
                            on='Crime ID', how='left'
                        )
                        logging.info(f"Successfully merged street and outcomes for region: {region}")
                    except KeyError as e:
                        logging.error(f"Error during merge for region {region}: {e}")
                else:
                    logging.warning(f"No matching outcomes DataFrame found for region {region}. Skipping merge.")
            except Exception as e:
                logging.error(f"Error processing key '{key}': {e}")
    
    logging.info("Finished merging street and outcomes DataFrames.")
    return merged_crime_df


def broad_outcome_categories(merged_crime_df, police_forces):
    """
    Combines 'Last outcome category' and 'Outcome type' for each region (based on police forces),
    replacing 'Unknown' values appropriately. Drops old outcome columns after the combination.

    Parameters:
    merged_crime_df (dict): A dictionary where keys are regions and values are the corresponding merged DataFrames.
    police_forces (list): A list of police forces (regions) to process.

    Returns:
    merged_crime_df (dict): Updated DataFrames with 'Combined Outcome' column and unnecessary columns dropped.
    """
    logging.info('Starting broad outcome category processing.')

    for region in police_forces:
        if region in merged_crime_df:
            df = merged_crime_df[region]
            logging.info(f'Processing region: {region}')
            
            # Combine 'Last outcome category' and 'Outcome type' with the conditions
            df['Combined Outcome'] = np.where(df['Last outcome category'].eq('Unknown'), 
                                              df['Outcome type'], 
                                              df['Last outcome category'])
            df['Combined Outcome'] = np.where(df['Outcome type'].eq('Unknown'), 
                                              df['Last outcome category'], 
                                              df['Combined Outcome'])
            df['Combined Outcome'] = df['Combined Outcome'].fillna('Unknown')  # Ensure all NaNs are turned into 'Unknown'

            # Drop the old columns if they exist
            df = df.drop(columns=['Last outcome category', 'Outcome type'], errors='ignore')

            # Update the dictionary with the modified DataFrame
            merged_crime_df[region] = df

            logging.info(f'Completed processing for region: {region}')
        else:
            logging.warning(f'Region {region} not found in merged_crime_df.')

    logging.info('Finished broad outcome category processing for all regions.')
    return merged_crime_df


def merge_postcode_with_data(data_df, staged_postcodes):
    """
    Primary Layer: Merges the nearest postcode information to each record (crime or stop-and-search)
    in data_df based on latitude and longitude using KDTree.

    Parameters:
        data_df (pd.DataFrame): The data (crime or stop-and-search) that contains latitude and longitude.
        staged_postcodes (pd.DataFrame): The staged postcode data that contains latitude, longitude, and postcode information.

    Returns:
        pd.DataFrame: The merged DataFrame with 'Postcode' information added.
    """
    logging.info("Merging nearest postcode information into data using staged postcodes.")

    # Ensure that 'Latitude' and 'Longitude' exist in data_df before proceeding
    if 'Latitude' not in data_df.columns or 'Longitude' not in data_df.columns:
        logging.error("Latitude and Longitude columns are missing from the data.")
        raise ValueError("Latitude and Longitude columns are required for postcode merging.")

    # Prepare the coordinate arrays
    postcode_coords = staged_postcodes[['Latitude', 'Longitude']].values
    data_coords = data_df[['Latitude', 'Longitude']].values

    # Build a KDTree for the postcodes
    tree = KDTree(postcode_coords)

    # Find the nearest postcode for each point in data_df
    _, indices = tree.query(data_coords)

    # Map the nearest indices to postcodes and add them to the data
    data_df['nearest_postcode'] = staged_postcodes.iloc[indices]['Postcode'].values

    # Calculate percentage of records where nearest postcode was mapped
    total_records = len(data_df)
    matched_records = data_df['nearest_postcode'].notna().sum()
    match_percentage = (matched_records / total_records) * 100

    logging.info(f"Nearest postcodes merged for {matched_records} out of {total_records} records "
                 f"({match_percentage:.2f}% match rate).")

    return data_df


def filter_house_price_by_county(staged_price_df):
    """
    Filters the house price DataFrame for just Suffolk and Norfolk counties.
    
    Parameters:
        house_price_df (pd.DataFrame): The house price DataFrame with a 'County' column.
        
    Returns:
        pd.DataFrame: Filtered DataFrame with only rows for Suffolk and Norfolk counties.
    """
    logging.info("Filtering house price data for Suffolk and Norfolk counties.")

    # Define the counties to filter
    counties = ['SUFFOLK', 'NORFOLK']

    # Filter the DataFrame to include only rows where 'County' is Suffolk or Norfolk
    filtered_price_df = staged_price_df[staged_price_df['County'].isin(counties)]
    
    logging.info(f"Filtered data contains {len(filtered_price_df)} rows for Suffolk and Norfolk counties.")
    
    return filtered_price_df


def combine_region_data(merged_crime_df, police_forces):
    """
    Combines data from multiple regions into a single DataFrame, adding a 'region' column to specify the region.

    Parameters:
        merged_crime_df (dict): Dictionary where keys are region names and values are DataFrames for each region.
        police_forces (list): List of police forces (regions) to combine (e.g., ['norfolk', 'suffolk']).

    Returns:
        pd.DataFrame: A combined DataFrame with data from all specified regions, 
                      including a 'region' column to specify the region.
    """
    logging.info(f"Combining data for the police forces: {police_forces}")

    combined_crime_df = pd.DataFrame()  # Initialize an empty DataFrame

    for region in police_forces:
        if region in merged_crime_df:
            logging.info(f"Processing data for {region}")
            region_df = merged_crime_df[region].copy()  # Copy the DataFrame to avoid modifying the original
            region_df['region'] = region.capitalize()  # Add the 'region' column with the region name
            combined_crime_df = pd.concat([combined_crime_df, region_df], ignore_index=True)  # Combine with the main DataFrame
        else:
            logging.warning(f"Data for region {region} not found in merged_crime_df.")

    logging.info(f"Combined data contains {len(combined_crime_df)} records from {len(police_forces)} regions.")

    return combined_crime_df


def combine_stop_and_search_data(staged_df, police_forces):
    """
    Combines stop-and-search data from multiple regions (e.g., Norfolk, Suffolk) into a single DataFrame,
    adding a 'region' column to specify the region.

    Parameters:
        staged_df (dict): Dictionary where keys are region names and values are DataFrames for each region's data.
        police_forces (list): List of police forces (regions) to combine (e.g., ['norfolk', 'suffolk']).

    Returns:
        pd.DataFrame: A combined DataFrame with stop-and-search data from all specified regions, 
                      including a 'region' column to specify the region.
    """
    # Log the keys available in the staged_df dictionary for debugging
    logging.info(f"Available keys in staged_df: {list(staged_df.keys())}")

    logging.info(f"Combining stop-and-search data for the police forces: {police_forces}")

    combined_search_df = pd.DataFrame()  # Initialize an empty DataFrame for stop-and-search data

    # Iterate through the police forces and look for the stop-and-search data
    for region in police_forces:
        key = f"staged_{region}-stop-and-search"  # Adjusting the key structure based on common conventions
        if key in staged_df:
            logging.info(f"Processing stop-and-search data for {region}")
            search_df = staged_df[key].copy()  # Copy the DataFrame to avoid modifying the original
            search_df['region'] = region.capitalize()  # Add the 'region' column with the region name
            combined_search_df = pd.concat([combined_search_df, search_df], ignore_index=True)  # Combine with the main DataFrame
        else:
            logging.warning(f"Stop-and-search data for {region} not found in staged_df with key: {key}.")

    logging.info(f"Combined stop-and-search data contains {len(combined_search_df)} records from {len(police_forces)} regions.")

    return combined_search_df


def check_for_nulls(df, df_name):
    """
    Check for null values (NaN or empty spaces) in a single DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame to check for null values.
        df_name (str): Name of the DataFrame (for logging purposes).

    Returns:
        None. Logs the null values check for the DataFrame.
    """
    # Check for NaN values and empty spaces
    logging.info(f"Checking for null values in {df_name}.")
    
    null_mask = df.isna() | df.apply(lambda x: x == '', axis=1)

    # Count total nulls per column
    null_counts = null_mask.sum()

    # Calculate total rows and percentage of null values
    total_rows = len(df)
    null_percentage = (null_counts / total_rows) * 100

    # Log details of columns with null values
    for col, count in null_counts.items():
        if count > 0:
            logging.info(f"{df_name}: Column '{col}' contains {count} null values "
                         f"({null_percentage[col]:.2f}% of total rows).")
        else:
            logging.info(f"{df_name}: Column '{col}' has no null values.")

    logging.info(f"Finished checking null values in {df_name}.")


def validate_postcode_merge(df_before_merge_count, df_after_merge, dataset_name):
    """
    Validates that no rows were lost or unexpectedly added after merging postcodes with the given dataset,
    and ensures that all records have valid postcodes after the merge.

    Parameters:
        df_before_merge_count (int): Row count of the DataFrame before the merge.
        df_after_merge (pd.DataFrame): DataFrame after the merge with postcodes.
        dataset_name (str): The name of the dataset being validated (e.g., 'crime' or 'stop-and-search').

    Raises:
        AssertionError: If the row count changes unexpectedly after the merge or if any record lacks a valid postcode.
    """
    df_after_merge_count = len(df_after_merge)
    
    # Validate that the row count hasn't changed unexpectedly
    assert df_before_merge_count == df_after_merge_count, (
        f"Row count mismatch after merging postcodes with {dataset_name} data: "
        f"Before: {df_before_merge_count}, After: {df_after_merge_count}"
    )
    
    # Check that all records have valid postcodes
    assert df_after_merge['nearest_postcode'].notna().all(), (
        f"Postcode merge failed for some records in {dataset_name} data. Some postcodes are missing."
    )
    
    logging.info(f"Postcode merge validation passed for {dataset_name} data: {df_after_merge_count} rows.")


def check_and_drop_duplicates(df, df_name):
    """
    Checks for and removes duplicate rows in the DataFrame, and reports the percentage of duplicates.
    
    Parameters:
    df (pd.DataFrame): The DataFrame to check for duplicates.
    df_name (str): A name or label for the DataFrame (for logging purposes).
    
    Returns:
    pd.DataFrame: The DataFrame with duplicate rows removed.
    """
    logging.info(f"Checking for duplicate rows in {df_name}.")
    
    total_rows = len(df)
    duplicate_rows = df.duplicated().sum()

    if duplicate_rows > 0:
        percentage = (duplicate_rows / total_rows) * 100
        logging.warning(f"Found {duplicate_rows} duplicate rows in {df_name}, which is {percentage:.2f}% of the total data.")
        
        # Drop the duplicates and log the result
        df = df.drop_duplicates()
        logging.info(f"Duplicate rows removed from {df_name}. Now the DataFrame contains {len(df)} rows.")
    else:
        logging.info(f"No duplicate rows found in {df_name}.")

    return df


def remove_invalid_age_ranges(df, invalid_value='Oct-17'):
    """
    Removes rows where the 'Age range' column contains a specific invalid value.

    Parameters:
        df (pd.DataFrame): The DataFrame to filter.
        invalid_value (str): The value in the 'Age range' column to filter out (default is 'Oct-17').

    Returns:
        pd.DataFrame: The filtered DataFrame.
    """
    logging.info(f"Removing rows where 'Age range' is {invalid_value}.")
    filtered_df = df[df['Age range'] != invalid_value]
    logging.info(f"Rows with 'Age range' as {invalid_value} removed.")
    
    return filtered_df

def load_primary_data(base_path):
    """
    Load primary layer data files for crime, house price, and stop-and-search datasets from CSV files.

    Parameters:
        base_path (str): The base path to the primary data files.

    Returns:
        tuple: DataFrames for crime, house price, and stop-and-search data.
    """
    logging.info("Loading primary layer data from CSV files.")

    # Define the file paths
    crime_file = os.path.join(base_path, 'primary_layer_data', 'primary_crime.csv')
    house_price_file = os.path.join(base_path, 'primary_layer_data', 'primary_house_price_data.csv')
    stop_search_file = os.path.join(base_path, 'primary_layer_data', 'primary_combined_stop_and_search.csv')

    # Load the CSV files
    logging.info(f"Loading crime data from {crime_file}")
    crime_df = pd.read_csv(crime_file)

    logging.info(f"Loading house price data from {house_price_file}")
    house_price_df = pd.read_csv(house_price_file)

    logging.info(f"Loading stop-and-search data from {stop_search_file}")
    stop_search_df = pd.read_csv(stop_search_file)

    logging.info("Primary layer data loaded successfully.")
    return crime_df, house_price_df, stop_search_df


def load_csv_data(base_path, file_name):
    """
    Load a CSV file from the provided base path and file name.

    Parameters:
        base_path (str): The base path where the file is located.
        file_name (str): The name of the CSV file to be loaded.

    Returns:
        pd.DataFrame: The loaded CSV data as a DataFrame.
    """
    logging.info(f"Loading CSV data from {file_name}.")

    try:
        # Construct the full path to the CSV file
        file_path = os.path.join(base_path, file_name)

        # Log the full file path for debugging
        logging.info(f"Loading data from {file_path}")

        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)

        logging.info(f"{file_name} loaded successfully.")
        return df
    except Exception as e:
        logging.error(f"Error loading {file_name}: {e}")
        raise


def outcode_crime_price(crime_df, house_price_df, staged_postcodes):
    """
    Aggregates crime data by outcode and calculates the average house price for each outcode, adding district as an extra column.

    Parameters:
        crime_df (pd.DataFrame): The crime data.
        house_price_df (pd.DataFrame): The house price data.
        staged_postcodes (pd.DataFrame): DataFrame containing postcode-to-district mapping.
        
    Returns:
        pd.DataFrame: A table with outcode, total crime count for the outcode, average house price, and district.
    """
    logging.info("Aggregating crime count and average house price by outcode.")

    # Extract the part before the space from the postcode to represent outcode (district) in crime data
    crime_df['Outcode'] = crime_df['nearest_postcode'].str.split(' ').str[0]

    # Extract the part before the space from the postcode in house price data
    house_price_df['Outcode'] = house_price_df['Postcode'].str.split(' ').str[0]

    # Calculate the crime count by outcode
    crime_count_df = crime_df.groupby('Outcode').size().reset_index(name='Crime Count')

    # Calculate the average house price by outcode
    house_price_avg_df = house_price_df.groupby('Outcode')['Price'].mean().reset_index(name='Avg House Price')

    # Merge crime count and average house price into a single DataFrame by outcode
    outcode_df = crime_count_df.merge(house_price_avg_df, on='Outcode', how='outer')

    # Add District information from staged_postcodes using outcode
    staged_postcodes['Outcode'] = staged_postcodes['Postcode'].str.split(' ').str[0]
    outcode_df = outcode_df.merge(staged_postcodes[['Outcode', 'District']].drop_duplicates(), on='Outcode', how='left')

    # Remove rows where Avg House Price is null (i.e., no house price information)
    outcode_df = outcode_df.dropna(subset=['Avg House Price'])

    logging.info("Crime count and average house price aggregation by outcode with district completed.")
    
    return outcode_df


def crime_vs_house_price_trends(crime_df, house_price_df):
    """
    Aggregates crime rates and house price trends over time.

    Parameters:
        crime_df (pd.DataFrame): The crime data.
        house_price_df (pd.DataFrame): The house price data.
        
    Returns:
        pd.DataFrame: Aggregated data showing crime rates and house prices over time.
    """
    logging.info("Aggregating crime rates and house price trends over time.")

    # Aggregate crime by month and year
    crime_trends = crime_df.groupby(['Year', 'Month']).size().reset_index(name='Crime Count')

    # Aggregate house prices by month and year
    house_price_trends = house_price_df.groupby(['Year', 'Month'])['Price'].mean().reset_index(name='Avg House Price')

    # Merge both on year and month
    trends_df = crime_trends.merge(house_price_trends, on=['Year', 'Month'], how='left')

    return trends_df


def stop_search_statistics(stop_search_df):
    """
    Aggregates stop-and-search data by demographic information.

    Parameters:
        stop_search_df (pd.DataFrame): The stop-and-search data.
        
    Returns:
        pd.DataFrame: Aggregated data showing stop-and-search statistics by age, gender, and ethnicity.
    """
    logging.info("Aggregating stop-and-search statistics by demographic information.")

    # Group by age range, gender, and ethnicity
    stop_search_stats = stop_search_df.groupby(['Age range', 'Gender', 'Self-defined ethnicity']).size().reset_index(name='Search Count')

    return stop_search_stats


def house_price_change_per_district(house_price_df, staged_postcodes_df):
    """
    Aggregates average house price per outcode over time, and adds a column for the district.

    Parameters:
        house_price_df (pd.DataFrame): The house price data.
        staged_postcodes (pd.DataFrame): DataFrame containing postcode-to-district mapping.
        
    Returns:
        pd.DataFrame: Aggregated data showing average house price per outcode and district over time.
    """
    logging.info("Aggregating house price changes per outcode and district over time.")

    # Extract the part before the space from the postcode for outcode
    house_price_df['Outcode'] = house_price_df['Postcode'].str.split(' ').str[0]

    # Group by Outcode, Year, and Month to calculate average house price
    price_trends_df = house_price_df.groupby(['Outcode', 'Year', 'Month'])['Price'].mean().reset_index(name='Avg House Price')

    # Merge with the District column from staged_postcodes using Outcode
    staged_postcodes_df['Outcode'] = staged_postcodes_df['Postcode'].str.split(' ').str[0]
    price_trends_df = price_trends_df.merge(staged_postcodes_df[['Outcode', 'District']].drop_duplicates(), on='Outcode', how='left')

    logging.info("House price changes per outcode and district aggregated successfully.")
    
    return price_trends_df


def region_summary_table(crime_df, stop_search_df, house_price_df, summary_stats_df):
    """
    Creates a summary table that aggregates data by region, including crime count, crime per 1000 people, 
    crime per £1M budget, crime per square mile, and average house price.

    Parameters:
        crime_df (pd.DataFrame): Crime data with 'region' column.
        stop_search_df (pd.DataFrame): Stop-and-search data with 'region' column.
        house_price_df (pd.DataFrame): House price data with 'County' column.
        summary_stats_df (pd.DataFrame): Summary statistics pivot table with population, area, and budget.

    Returns:
        pd.DataFrame: Aggregated table with region-wise summary statistics.
    """
    logging.info("Creating region summary table with crime and house price data.")

    # Transpose summary_stats_df so that regions become a column
    summary_stats_transposed = summary_stats_df.T
    summary_stats_transposed.columns = summary_stats_transposed.iloc[0]  # Set the first row as column headers
    summary_stats_transposed = summary_stats_transposed.drop(summary_stats_transposed.index[0])  # Drop the first row
    summary_stats_transposed = summary_stats_transposed.reset_index().rename(columns={'index': 'region'})
    
    # Convert population and area to numeric
    summary_stats_transposed['population'] = summary_stats_transposed['population'].str.replace(',', '').astype(float)
    summary_stats_transposed['area'] = summary_stats_transposed['area'].str.replace(',', '').astype(float)
    
    # Clean the budget column: remove £ sign and commas, then convert to float
    summary_stats_transposed['budget'] = summary_stats_transposed['budget'].str.replace('£', '').str.replace(',', '').astype(float)

    # Aggregate crime count per region
    crime_region_df = crime_df.groupby('region').size().reset_index(name='Crime Count')
    
    # Aggregate house price per county (to be matched with regions)
    avg_house_price_df = house_price_df.groupby('County')['Price'].mean().reset_index(name='Avg House Price')
    
    # Merge the crime data with summary stats (population, area, budget)
    summary_df = crime_region_df.merge(summary_stats_transposed, on='region', how='left')
    
    # Merge the house price data with the summary dataframe
    summary_df = summary_df.merge(avg_house_price_df, left_on='region', right_on='County', how='left')

    # Calculate additional columns
    summary_df['Crime per 1000'] = summary_df['Crime Count'] / (summary_df['population'] / 1000)
    summary_df['Crime per £1M Budget'] = summary_df['Crime Count'] / (summary_df['budget'] / 1_000_000)
    summary_df['Crime per Square Mile'] = summary_df['Crime Count'] / summary_df['area']

    # Remove unnecessary columns (like 'County' from the merge)
    summary_df.drop(columns=['County'], inplace=True)

    logging.info("Region summary table created successfully.")
    
    return summary_df


def crime_outcome_by_region(crime_df):
    """
    Aggregates crime data by crime type, combined outcome, and region.

    Parameters:
        crime_df (pd.DataFrame): The crime data with 'Crime type', 'Combined Outcome', and 'region' columns.
        
    Returns:
        pd.DataFrame: A table with crime type, combined outcome, region, and the count of occurrences.
    """
    logging.info("Aggregating crime data by crime type, combined outcome, and region.")

    # Grouping by 'Crime type', 'Combined Outcome', and 'region', and calculating the count of occurrences
    crime_region_outcome_df = crime_df.groupby(['Crime type', 'Combined Outcome', 'region']).size().reset_index(name='Crime Count')

    logging.info("Crime data aggregation by crime type, combined outcome, and region completed.")

    return crime_region_outcome_df




#Staging Layer
"""
Key Aspects of Staging:
Ingesting raw data from various sources.
Basic data cleaning to ensure data is usable (e.g., removing NaN values, correcting basic formatting issues).
Standardizing the data format so it’s ready for further transformations 
(e.g., splitting date columns, ensuring column consistency).
Validations to ensure the data meets expected quality standards.
"""
def staging_layer(base_path, police_forces):
    """
    Staging Layer: Orchestrates the ingestion, cleaning, transformation, and staging of crime data, house price data,
    and postcode data into CSV files. The data includes crime records, house price transactions, and postcode information.

    Parameters:
        base_path (str): Base directory where all datasets (crime, house price, postcodes) are stored.
        police_forces (list): List of police forces to include in the crime data ingestion (e.g., 'norfolk', 'suffolk').
    """
    logging.info("Starting the data staging process.")

    try:
        # Step 1: Ingest raw data
        logging.info('Ingesting raw crime data from the specified police forces.')
        crime_data = import_crime_data(base_path, police_forces)
    
        logging.info('Ingesting raw house price data from the source.')
        house_price_data = load_house_price_data(base_path)

        logging.info(f"Loaded crime data for {len(crime_data)} police forces.")
        logging.info(f"Loaded {len(house_price_data)} years of house price data.")

        # Step 2: Apply headers to the house price data
        logging.info('Applying predefined column headers to house price data.')
        house_price_data = apply_house_price_column_headers(house_price_data)

        # Step 3: Combine datasets by year for crime and house price data
        logging.info('Combining crime data from multiple years for each police force.')
        combined_crime_df = combine_dataframes(crime_data)

        logging.info("Combining house price data from different years into a single DataFrame.")
        combined_price_df = combine_house_price_data(house_price_data)

        # Step 4: Clean and transform crime data
        logging.info("Beginning the cleaning and transformation process for crime data.")

        # 4.1: Drop empty columns and remove duplicate rows
        logging.info("Dropping empty columns and removing duplicate rows from the crime data.")
        combined_crime_df = check_and_drop_empty_columns(remove_duplicates(combined_crime_df))

        # 4.2: Split date column into 'Day', 'Month', and 'Year' for crime data
        logging.info("Splitting date columns into 'Day', 'Month', and 'Year' in the crime data.")
        combined_crime_df = split_date_columns(combined_crime_df)

        # 4.3: Remove rows with missing values in critical columns (Latitude, Longitude, etc.)
        logging.info("Removing rows with missing data in critical columns (Latitude, Longitude, LSOA code, etc.) from the crime data.")
        critical_columns = ['Latitude', 'Longitude', 'LSOA code', 'LSOA name']
        combined_crime_df = remove_rows_with_missing_data(combined_crime_df, critical_columns)

        # 4.4: Replace NaN values with 'Unknown' for specified columns
        logging.info("Replacing NaN values with 'Unknown' in relevant columns in the crime data.")
        crime_columns_to_replace = [
            'Gender', 'Age range', 'Self-defined ethnicity', 'Officer-defined ethnicity', 'Object of search',
            'Outcome linked to object of search', 'Outcome type', 'Removal of more than just outer clothing',
            'Crime ID', 'Last outcome category', 'Part of a policing operation', 'Legislation', 'Outcome'
        ]
        combined_crime_df = replace_nans_with_unknown(combined_crime_df, crime_columns_to_replace)
        
        # 4.5: Remove unnecessary columns from the crime dataset
        logging.info('Dropping unnecessary columns from the crime data.')
        columns_to_remove = ['Reported by', 'Falls within']
        combined_crime_df = drop_unnecessary_columns(combined_crime_df, columns_to_remove)

        # Step 5: Clean and transform house price data
        logging.info("Beginning the cleaning and transformation process for house price data.")

        # 5.1: Remove duplicate rows in the house price dataset
        logging.info("Removing duplicate rows from the house price data.")
        combined_price_df = remove_duplicates_from_combined_price_df(combined_price_df)

        # 5.2: Remove rows with missing values in essential columns for house price data
        logging.info("Removing rows with missing values in critical columns (Price, Postcode, Property Type, etc.) from the house price data.")
        columns_to_check = ['Price', 'Date of Transfer', 'Postcode', 'Property Type', 'Old/New', 'Duration']
        combined_price_df = remove_rows_with_missing_values(combined_price_df, columns_to_check)

        # 5.3: Split 'Date of Transfer' column into 'Day', 'Month', and 'Year'
        logging.info("Splitting 'Date of Transfer' into 'Day', 'Month', and 'Year' in the house price data.")
        combined_price_df = clean_and_split_date_columns(combined_price_df)

        # 5.4: Remove unnecessary columns from house price data
        logging.info('Dropping unnecessary columns from house price data.')
        columns_to_remove = ['Duration', 'PAON', 'SAON', 'Street', 'Locality', 'PPD Category Type', 'Record Status']
        combined_price_df = drop_unnecessary_columns(combined_price_df, columns_to_remove)

        # 5.5: Replace keys for property type and old/new with the full information and remove invalid rows
        logging.info("Replacing abbreviated Property Type and Old/New keys with their full descriptions and removing invalid rows.")
        combined_price_df = replace_property_type_and_old_new_keys(combined_price_df)

        # Step 6: Load and stage postcode data
        logging.info('Ingesting and staging postcode data.')
        staged_postcodes = stage_postcode_data(base_path)

        # Step 7: Data type correction and validation
        logging.info('Correcting and validating data types for the staged data.')

        # 7.1: Define expected data types
        crime_expected_types = {
            'Crime ID': str, 'Month': int, 'Longitude': float, 'Latitude': float,
            'Location': str, 'LSOA code': str, 'LSOA name': str, 'Crime type': str, 'Year': int
        }
        search_expected_types = {
            'Type': str, 'Part of a policing operation': str, 'Latitude': float, 
            'Longitude': float, 'Gender': str, 'Age range': str, 'Self-defined ethnicity': str, 
            'Officer-defined ethnicity': str, 'Legislation': str, 'Object of search': str, 
            'Outcome': str, 'Outcome linked to object of search': str, 
            'Removal of more than just outer clothing': str, 'Year': int, 'Month': int, 'Day': int
        }
        price_expected_types = {
            'Transaction unique identifier': str, 'Price': int, 'Postcode': str, 
            'Property Type': str, 'Old/New': str, 'Town/City': str, 'District': str, 
            'County': str, 'Day': int, 'Month': int, 'Year': int
        }
        postcode_expected_types = {
            'Postcode': str, 'In Use?': str, 'Latitude': float, 'Longitude': float
        }

        # 7.2 Correct data types
        for region, df in combined_crime_df.items():
            if region in ['norfolk-street', 'suffolk-street', 'norfolk-outcomes', 'suffolk-outcomes']:
                combined_crime_df[region] = correct_data_types(df, crime_expected_types, f"{region} Crime Data")
            elif region in ['norfolk-stop-and-search', 'suffolk-stop-and-search']:
                combined_crime_df[region] = correct_data_types(df, search_expected_types, f"{region} Stop-and-Search Data")
        combined_price_df = correct_data_types(combined_price_df, price_expected_types, "House Price Data")
        staged_postcodes = correct_data_types(staged_postcodes, postcode_expected_types, "Postcode Data")

        # 7.3 Validate data types
        for region, df in combined_crime_df.items():
            if region in ['norfolk-street', 'suffolk-street', 'norfolk-outcomes', 'suffolk-outcomes']:
                validate_data_types(df, crime_expected_types, f"{region} Crime Data")
            elif region in ['norfolk-stop-and-search', 'suffolk-stop-and-search']:
                validate_data_types(df, search_expected_types, f"{region} Stop-and-Search Data")
        validate_data_types(combined_price_df, price_expected_types, 'House Price Data')
        validate_data_types(staged_postcodes, postcode_expected_types, 'Postcode Data')
        logging.info("Data type validation completed.")

        # Step 8: Save the cleaned and transformed datasets to CSV files in the 'staged_data' directory
        logging.info("Saving the cleaned and staged data to CSV files.")

        # 8.1: Create 'staged_data' directory if it does not exist
        staged_dir = os.path.join(base_path, 'staged_data')
        os.makedirs(staged_dir, exist_ok=True)

        # 8.2: Save staged crime data to CSV files
        logging.info("Saving staged crime data to CSV files.")
        for key, df in combined_crime_df.items():
            output_file = os.path.join(staged_dir, f"staged_{key}.csv")
            df.to_csv(output_file, index=False)
            logging.info(f"Staged crime data saved to {output_file}.")

        # 8.3: Save staged postcode data to CSV
        logging.info("Saving staged postcode data to CSV.")
        staged_postcode_file = os.path.join(staged_dir, 'staged_postcodes.csv')
        staged_postcodes.to_csv(staged_postcode_file, index=False)
        logging.info(f"Staged postcode data saved to {staged_postcode_file}.")

        # 8.4: Save staged house price data to CSV
        logging.info("Saving staged house price data to CSV.")
        staged_house_price_file = os.path.join(staged_dir, 'staged_house_price_data.csv')
        combined_price_df.to_csv(staged_house_price_file, index=False)
        logging.info(f"Staged house price data saved to {staged_house_price_file}.")

        logging.info("Data staging process completed successfully.")

    except Exception as e:
        logging.error(f"An error occurred during the data staging process: {e}")
        raise


#Primary Layer
"""
Key Aspects of Primary Layer:
- Loading staged data for crime, house prices, and postcodes.
- Merging relevant datasets (e.g., combining street and outcomes data for crime).
- Applying higher-level transformations and category assignments (e.g., broad outcome categories).
- Cleaning and validating columns to ensure consistency and readiness for reporting or analysis.
- Merging postcode information with crime data based on geographic coordinates.
- Storing the final processed data in the primary layer for further analysis, visualization, or reporting.
"""

def primary_layer(base_path, police_forces, data_types):
    """
    Processes staged data by merging, transforming, and saving it as primary layer data.
    This includes crime, stop-and-search, and house price data, applying transformations 
    and saving the final datasets to CSV files in the primary layer.

    Parameters:
        base_path (str): Base directory where staged data is stored.
        police_forces (list): List of police forces to process (e.g., 'norfolk', 'suffolk').
        data_types (list): List of data types to process (e.g., 'street', 'outcomes', 'stop-and-search').
    """
    logging.info("Primary layer processing initiated.")

    try:
        # Step 1: Load staged data
        logging.info("Loading staged data for crime, house prices, and postcodes.")
        staged_df, staged_price_df, staged_postcodes = load_staged_data(base_path, police_forces, data_types)

        logging.info(f"Loaded {len(staged_df)} staged crime datasets.")
        logging.info(f"Loaded {len(staged_price_df)} house price records.")
        logging.info(f"Loaded {len(staged_postcodes)} postcode records.")

        # Step 2: Process and merge crime data (street and outcomes)
        logging.info("Merging street and outcomes data for each police force.")
        merged_crime_df = merge_street_outcomes(staged_df)

        logging.info("Applying broad outcome categories to the crime data.")
        merged_crime_df = broad_outcome_categories(merged_crime_df, police_forces)

        logging.info("Combining crime data across regions (Norfolk and Suffolk).")
        combined_crime_df = combine_region_data(merged_crime_df, police_forces)

        # Step 3: Process stop-and-search data
        logging.info("Processing stop-and-search data.")
        logging.info("Removing invalid 'Age range' entries from stop-and-search data.")
        staged_df['staged_norfolk-stop-and-search'] = remove_invalid_age_ranges(staged_df['staged_norfolk-stop-and-search'])
        staged_df['staged_suffolk-stop-and-search'] = remove_invalid_age_ranges(staged_df['staged_suffolk-stop-and-search'])
        
        combined_search_df = combine_stop_and_search_data(staged_df, police_forces)

        # Step 4: Filter house price data by county
        logging.info("Filtering house price data for Suffolk and Norfolk counties.")
        filtered_price_df = filter_house_price_by_county(staged_price_df)
        logging.info(f"Filtered house price data contains {len(filtered_price_df)} records.")

        # Step 5: Merge postcodes with crime data
        logging.info("Merging postcode data with crime data.")
        crime_df_before_postcode_merge_count = len(combined_crime_df)
        combined_crime_df = merge_postcode_with_data(combined_crime_df, staged_postcodes)
        validate_postcode_merge(crime_df_before_postcode_merge_count, combined_crime_df, "crime")
        logging.info("Postcode merge validation completed for crime data.")

        # Step 6: Merge postcodes with stop-and-search data
        logging.info("Merging postcode data with stop-and-search data.")
        search_df_before_postcode_merge_count = len(combined_search_df)
        combined_search_df = merge_postcode_with_data(combined_search_df, staged_postcodes)
        validate_postcode_merge(search_df_before_postcode_merge_count, combined_search_df, "stop-and-search")
        logging.info("Postcode merge validation completed for stop-and-search data.")

        # Step 7: Validate for null values across datasets
        logging.info("Checking for null values in crime, stop-and-search, and house price datasets.")
        check_for_nulls(combined_crime_df, "Combined Crime Data")
        check_for_nulls(combined_search_df, "Combined Stop-and-Search Data")
        check_for_nulls(filtered_price_df, "Filtered House Price Data")
        logging.info("Null value validation completed.")

        # Step 8: Remove duplicate rows
        logging.info("Checking for and removing duplicate rows across datasets.")
        combined_crime_df = check_and_drop_duplicates(combined_crime_df, "Combined Crime Data")
        combined_search_df = check_and_drop_duplicates(combined_search_df, "Combined Stop-and-Search Data")
        filtered_price_df = check_and_drop_duplicates(filtered_price_df, "Filtered House Price Data")
        logging.info("Duplicate row removal completed.")

        # Step 9: Correct data types after transformations
        logging.info("Correcting data types in crime, stop-and-search, and house price datasets.")
        primary_crime_expected_types = {
            'Crime ID': str, 'Month': int, 'Longitude': float, 'Latitude': float,
            'Location': str, 'LSOA code': str, 'LSOA name': str, 'Crime type': str,
            'Year': int, 'Combined Outcome': str, 'region': str, 'nearest_postcode': str
        }
        primary_search_expected_types = {
            'Type': str, 'Part of a policing operation': str, 
            'Latitude': float, 'Longitude': float, 'Gender': str, 'Age range': str, 
            'Self-defined ethnicity': str, 'Officer-defined ethnicity': str, 
            'Legislation': str, 'Object of search': str, 'Outcome': str, 
            'Outcome linked to object of search': str, 'Removal of more than just outer clothing': str, 
            'Year': int, 'Month': int, 'Day': int, 'region': str, 'nearest_postcode': str
        }
        primary_price_expected_types = {
            'Transaction unique identifier': str, 'Price': int, 'Postcode': str,
            'Property Type': str, 'Old/New': str, 'Town/City': str, 'District': str,
            'County': str, 'Day': int, 'Month': int, 'Year': int
        }
        combined_crime_df = correct_data_types(combined_crime_df, primary_crime_expected_types, "Combined Crime Data")
        combined_search_df = correct_data_types(combined_search_df, primary_search_expected_types, "Combined Stop-and-Search Data")
        filtered_price_df = correct_data_types(filtered_price_df, primary_price_expected_types, "Filtered House Price Data")
        logging.info("Data type correction completed.")

        # Step 10: Validate data types in primary datasets
        logging.info("Validating data types for crime, stop-and-search, and house price datasets.")
        validate_data_types(combined_crime_df, primary_crime_expected_types, "Combined Crime Data")
        validate_data_types(combined_search_df, primary_search_expected_types, "Combined Stop-and-Search Data")
        validate_data_types(filtered_price_df, primary_price_expected_types, "Filtered House Price Data")
        logging.info("Data type validation completed.")

        # Step 11: Save combined crime data to CSV
        logging.info("Saving combined crime data to CSV in the primary layer.")
        primary_data_dir = os.path.join(base_path, 'primary_layer_data')
        os.makedirs(primary_data_dir, exist_ok=True)
        primary_crime_file = os.path.join(primary_data_dir, 'primary_crime.csv')
        combined_crime_df.to_csv(primary_crime_file, index=False)
        logging.info(f"Combined crime data saved to {primary_crime_file}.")

        # Step 12: Save combined stop-and-search data to CSV
        logging.info("Saving combined stop-and-search data to CSV in the primary layer.")
        stop_and_search_file = os.path.join(primary_data_dir, 'primary_combined_stop_and_search.csv')
        combined_search_df.to_csv(stop_and_search_file, index=False)
        logging.info(f"Combined stop-and-search data saved to {stop_and_search_file}.")

        # Step 13: Save filtered house price data to CSV
        logging.info("Saving filtered house price data to CSV in the primary layer.")
        primary_price_file = os.path.join(primary_data_dir, 'primary_house_price_data.csv')
        filtered_price_df.to_csv(primary_price_file, index=False)
        logging.info(f"Filtered house price data saved to {primary_price_file}.")

        logging.info("Primary layer processing completed successfully.")

    except Exception as e:
        logging.error(f"Error during primary layer processing: {e}")
        raise


#Reporting Layer
"""
Key Aspects of Reporting Layer:
- Aggregating crime and house price data by region and outcode, and calculating metrics such as average house price and crime count.
- Creating trend analysis tables that show how crime and house prices change over time across districts and regions.
- Generating region-based summary tables that include metrics like crime per 1000 people, crime per £1M budget, and crime per square mile using population, area, and budget data from summary statistics.
- Analyzing stop-and-search statistics by demographic attributes (e.g., gender, age, ethnicity), providing insights into search outcomes across different groups.
- Outputting the final, cleaned, and structured datasets into CSV files, ready for reporting and visualization.
- Ensuring data consistency by validating that all records are properly aggregated and ensuring there are no missing values in critical fields.
"""

def reporting_layer(base_path):
    """
    Reporting Layer: Aggregates and generates reporting data from primary datasets and saves the results to CSV.

    Parameters:
        base_path (str): Base directory where all datasets (crime, house price, postcodes) are stored.
    """
    logging.info("Reporting Layer: Starting the reporting process.")

    try:
        # Step 1: Load primary data
        logging.info("Loading primary data for crime, house price, and stop-and-search datasets.")
        crime_df, house_price_df, stop_search_df = load_primary_data(base_path)
        
        # Step 2: Load summary stats and staged postcodes for reporting
        logging.info("Loading summary statistics for reporting.")
        summary_stats_df = load_csv_data(base_path, 'summary_stats.csv')

        logging.info("Loading staged postcodes for reporting.")
        staged_postcodes_df = load_csv_data(os.path.join(base_path, 'staged_data'), 'staged_postcodes.csv')

        # Step 3: Aggregate crime and house price by district
        logging.info("Aggregating crime count and average house price by district.")
        outcode_df = outcode_crime_price(crime_df, house_price_df, staged_postcodes_df)

        # Step 4: Analyze crime and house price trends over time
        logging.info("Aggregating crime and house price trends over time.")
        trends_df = crime_vs_house_price_trends(crime_df, house_price_df)

        # Step 5: Analyze stop-and-search statistics by demographic
        logging.info("Aggregating stop-and-search data by demographic groups.")
        stop_search_stats_df = stop_search_statistics(stop_search_df)

        # Step 6: Aggregate house price changes per district over time
        logging.info("Aggregating house price changes per district over time.")
        price_trends_df = house_price_change_per_district(house_price_df, staged_postcodes_df)

        # Step 7: Create and save region summary table with crime, house price, and summary stats
        logging.info("Creating region summary table using crime data, house price data, and summary statistics.")
        summary_df = region_summary_table(crime_df, stop_search_df, house_price_df, summary_stats_df)

        # Step 8: Create a table of crime type, combined outcome, and region
        logging.info("Aggregating crime data by crime type, combined outcome, and region.")
        crime_outcome_region_df = crime_outcome_by_region(crime_df)

        # Step 9: Prepare the directory for saving results
        logging.info("Preparing directory for saving aggregated reporting data.")
        reporting_data_dir = os.path.join(base_path, 'reporting_layer_data')
        os.makedirs(reporting_data_dir, exist_ok=True)

        # Step 10: Save aggregated datasets to CSV in 'reporting_layer_data'
        logging.info("Saving aggregated data to CSV files.")

        # Define the file paths for saving each aggregated dataset
        outcode_file = os.path.join(reporting_data_dir, 'outcode_crime_price.csv')
        trends_file = os.path.join(reporting_data_dir, 'crime_vs_house_price_trends.csv')
        stop_search_file = os.path.join(reporting_data_dir, 'stop_search_stats_by_demographic.csv')
        house_price_trend_file = os.path.join(reporting_data_dir, 'house_price_change_per_district.csv')
        region_summary_file = os.path.join(reporting_data_dir, 'region_summary.csv')
        crime_outcome_region_file = os.path.join(reporting_data_dir, 'crime_outcome_by_region.csv')

        # Save DataFrames to CSV
        outcode_df.to_csv(outcode_file, index=False)
        trends_df.to_csv(trends_file, index=False)
        stop_search_stats_df.to_csv(stop_search_file, index=False)
        price_trends_df.to_csv(house_price_trend_file, index=False)
        summary_df.to_csv(region_summary_file, index=False)
        crime_outcome_region_df.to_csv(crime_outcome_region_file, index=False)

        logging.info("All aggregated datasets saved successfully.")

    except Exception as e:
        logging.error(f"Error occurred during the reporting layer process: {e}")
        raise

#Main
"""
Key Aspects of Main:

- Orchestrating the execution of different layers (staging, primary, reporting) in the pipeline.
- Providing flexibility to execute the entire pipeline or specific layers based on user input.
- Handling errors and exceptions to ensure smooth execution and meaningful logging.
- Ensuring the pipeline runs in the correct sequence (e.g., staging before primary, primary before reporting).
- Managing the flow of data across the pipeline, coordinating the transition between different processing stages.
"""

def main(pipeline='all'):
    """
    Main function to orchestrate the execution of the pipeline.
    
    Parameters:
        pipeline (str): Specify the part of the pipeline to run ('staging', 'primary', 'reporting', or 'all').
                        Default is 'all', which runs the full pipeline.
    """
    logging.info("Pipeline execution started.")

    base_path = r'EastofEngland_Dataset'
    police_forces = ['norfolk', 'suffolk']
    data_types = ['street', 'outcomes', 'stop-and-search']

    try:
        if pipeline in ['all', 'staging', 'primary', 'reporting']:
            # Run the Staging layer
            if pipeline in ['all', 'staging']:
                logging.info("Starting the Staging Layer.")
                staging_layer(base_path, police_forces)
                logging.info("Staging execution completed successfully.")

                if pipeline == 'staging':
                    logging.info("Pipeline run complete for Staging.")
                    return

            # Run the Primary layer
            if pipeline in ['all', 'primary']:
                logging.info("Starting the Primary Layer.")
                primary_layer(base_path, police_forces, data_types)
                logging.info("Primary execution completed successfully.")

                if pipeline == 'primary':
                    logging.info("Pipeline run complete for Primary.")
                    return

            # Run the Reporting layer
            if pipeline in ['all', 'reporting']:
                logging.info("Starting the Reporting Layer.")
                reporting_layer(base_path)
                logging.info("Reporting execution completed successfully.")

                if pipeline == 'reporting':
                    logging.info("Pipeline run complete for Reporting.")
                    return

            logging.info("Full pipeline execution completed successfully.")
        else:
            logging.critical("Invalid pipeline stage specified. Choose 'staging', 'primary', 'reporting', or 'all'.")

    except Exception as e:
        logging.error(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    main()