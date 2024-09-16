"""
List of Functions to be Tested in the Data Pipeline:
Note: Due to time constraints not all functions have been tested; ideally all of them should be tested.

### Staging Layer Functions:
1. `import_crime_data(base_path, police_forces)`:
   Imports crime data CSV files for specified police forces from the base directory.

2. `load_house_price_data(base_path)`:
   Loads house price data from CSV files in the base directory.

3. `combine_dataframes(crime_data)`:
   Combines crime data for each police force into a single DataFrame.

4. `check_and_drop_empty_columns(combined_crime_df)`:
   Identifies and removes columns that are entirely empty (NaN) from crime data.

5. `remove_duplicates(combined_crime_df)`:
   Removes duplicate rows from each crime DataFrame.

6. `clean_text_columns(combined_crime_df)`:
   Standardizes text columns (e.g., 'Location', 'Crime type') by converting to lowercase and stripping spaces.

7. `split_date_columns(combined_crime_df)`:
   Splits 'Month' and 'Year' for street and outcome data and adds 'Day' for stop-and-search data.

8. `remove_rows_with_missing_data(combined_crime_df, columns_to_check)`:
   Removes rows where specific critical columns (e.g., 'Latitude', 'Longitude') contain NaN values.

9. `replace_nans_with_unknown(data, columns_to_replace)`:
   Replaces NaN and empty values in specified columns with 'Unknown'.

10. `stage_postcode_data(base_path)`:
    Processes postcode data by selecting relevant columns and filtering based on the 'In Use' status.

11. `apply_house_price_column_headers(house_price_data)`:
    Applies appropriate column headers to the house price DataFrames.

12. `combine_house_price_data(house_price_data)`:
    Combines house price data from multiple years into a single DataFrame.

13. `remove_duplicates_from_combined_price_df(combined_price_df)`:
    Removes duplicate rows from the combined house price DataFrame.

14. `clean_and_split_date_columns(combined_price_df)`:
    Splits 'Date of Transfer' into 'Day', 'Month', and 'Year' in the house price data.

15. `drop_unnecessary_columns(data, columns_to_drop)`:
    Drops unnecessary columns from crime or house price data.

16. `remove_rows_with_missing_values(combined_price_df, columns_to_check)`:
    Removes rows with NaN values in specified columns from house price data.

17. `replace_property_type_and_old_new_keys(combined_price_df)`:
    Replaces abbreviated values for 'Property Type' and 'Old/New' with full descriptions in house price data.

18. `correct_data_types(df, expected_types, df_name)`:
    Corrects the data types of columns in the DataFrame based on expected types.

19. `validate_data_types(data, expected_types, dataset_name)`:
    Validates that the data types of columns match the expected types in the DataFrame.

20. `load_staged_data(base_path, police_forces, data_types)`:
    Loads staged crime, house price, and postcode data from the 'staged_data' folder.

### Primary Layer Functions:
21. `merge_street_outcomes(staged_df)`:
    Merges 'Outcome type' from outcomes data into street crime data for each police force.

22. `broad_outcome_categories(merged_crime_df, police_forces)`:
    Combines 'Last outcome category' and 'Outcome type' into a new 'Combined Outcome' column.

23. `merge_postcode_with_data(data_df, staged_postcodes)`:
    Merges nearest postcode information with crime or stop-and-search data based on latitude and longitude.

24. `filter_house_price_by_county(staged_price_df)`:
    Filters house price data to include only records for Suffolk and Norfolk counties.

25. `combine_region_data(merged_crime_df, police_forces)`:
    Combines crime data from multiple regions (e.g., Norfolk, Suffolk) into a single DataFrame.

26. `combine_stop_and_search_data(staged_df, police_forces)`:
    Combines stop-and-search data from multiple regions into a single DataFrame.

27. `check_for_nulls(df, df_name)`:
    Checks for null values in a given DataFrame and logs details of columns containing nulls.

28. `validate_postcode_merge(df_before_merge_count, df_after_merge, dataset_name)`:
    Validates that no rows were lost or added during postcode merging and ensures all records have valid postcodes.

29. `check_and_drop_duplicates(df, df_name)`:
    Identifies and removes duplicate rows in the given DataFrame.

30. `remove_invalid_age_ranges(df, invalid_value='Oct-17')`:
    Removes rows with specific invalid 'Age range' values from stop-and-search data.

31. `load_primary_data(base_path)`:
    Loads primary layer data (crime, house price, stop-and-search) from CSV files.

### Reporting Layer Functions:
32. `outcode_crime_price(crime_df, house_price_df, staged_postcodes)`:
    Aggregates crime data by outcode and calculates the average house price for each outcode.

33. `crime_vs_house_price_trends(crime_df, house_price_df)`:
    Aggregates crime rates and house price trends over time.

34. `stop_search_statistics(stop_search_df)`:
    Aggregates stop-and-search data by demographic information (age, gender, ethnicity).

35. `house_price_change_per_district(house_price_df, staged_postcodes_df)`:
    Aggregates average house price changes per district over time.

36. `region_summary_table(crime_df, stop_search_df, house_price_df, summary_stats_df)`:
    Creates a summary table by region, including crime count, crime per 1000 people, budget, and average house price.

37. `crime_outcome_by_region(crime_df)`:
    Aggregates crime data by crime type, combined outcome, and region.

38. `load_csv_data(base_path, file_name)`:
    Loads a specific CSV file from the base path into a DataFrame.

### Main Pipeline Functions:
39. `staging_layer(base_path, police_forces)`:
    Orchestrates the ingestion, cleaning, and transformation of data in the staging layer.

40. `primary_layer(base_path, police_forces, data_types)`:
    Orchestrates data merging, transformation, and saving for the primary layer.

41. `reporting_layer(base_path)`:
    Aggregates reporting data and saves the results to CSV files.

42. `main(pipeline='all')`:
    Main function that orchestrates the execution of the entire pipeline or specific layers (staging, primary, reporting).
"""

#Packages to import
import pytest
from unittest import mock
import os
import pandas as pd
import logging

# Importing all functions from the pipeline script for testing
from YanYu_Crime_Pipeline_Final import (
    import_crime_data,
    load_house_price_data,
    combine_dataframes,
    check_and_drop_empty_columns,
    remove_duplicates,
    clean_text_columns,
    split_date_columns,
    remove_rows_with_missing_data,
    replace_nans_with_unknown,
    stage_postcode_data,
    apply_house_price_column_headers,
    combine_house_price_data,
    remove_duplicates_from_combined_price_df,
    clean_and_split_date_columns,
    drop_unnecessary_columns,
    remove_rows_with_missing_values,
    replace_property_type_and_old_new_keys,
    correct_data_types,
    validate_data_types,
    load_staged_data,
    merge_street_outcomes,
    broad_outcome_categories,
    merge_postcode_with_data,
    filter_house_price_by_county,
    combine_region_data,
    combine_stop_and_search_data,
    check_for_nulls,
    validate_postcode_merge,
    check_and_drop_duplicates,
    remove_invalid_age_ranges,
    load_primary_data,
    load_csv_data,
    outcode_crime_price,
    crime_vs_house_price_trends,
    stop_search_statistics,
    house_price_change_per_district,
    region_summary_table,
    crime_outcome_by_region,
    staging_layer,
    primary_layer,
    reporting_layer,
    main
)

"""
Testing function 1: 'import_crime_data'
----------------------------------------

This test suite validates the functionality of the 'import_crime_data' function, ensuring that crime data is imported correctly from valid CSV files in the 'Crime_Dataset' directory. It covers multiple scenarios, including handling empty subfolders, file reading, and ensuring directories and files exist.

Included tests:

1. **Basic File Import**:
    - Verifies that the function correctly loads CSV files from the specified directory and returns a dictionary containing DataFrames with the correct keys.

2. **Empty Subfolder Handling**:
    - Ensures that the function can handle subfolders that contain no CSV files and skips them while still processing valid subfolders.

Each test includes assertions to check that the correct files are read, appropriate logs are generated, and that the dictionary of crime data is returned with the expected structure.
"""

@mock.patch('os.listdir')
@mock.patch('pandas.read_csv')
@mock.patch('os.path.isdir')
@mock.patch('os.path.exists')
def test_import_crime_data(mock_exists, mock_isdir, mock_read_csv, mock_listdir):
    """
    Test case for the 'import_crime_data' function.
    Verifies the function's ability to import crime data from valid CSV files.
    """
    
    # Simulate that the dataset directory exists
    mock_exists.return_value = True
    
    # Mock directory structure and file listing
    mock_listdir.side_effect = [
        ['2021-09'],  # Mock the folder name inside 'Crime_Dataset'
        ['2021-09-norfolk-street.csv', '2021-09-suffolk-outcomes.csv']  # Mock CSV files within the folder
    ]
    
    mock_isdir.return_value = True  # Simulate that directories exist

    # Mock CSV reading to return predefined DataFrames
    mock_read_csv.side_effect = [
        pd.DataFrame({'Crime ID': [1, 2]}),  # DataFrame for 'norfolk' file
        pd.DataFrame({'Crime ID': [3, 4]})   # DataFrame for 'suffolk' file
    ]

    # Call the function with mocked values
    base_path = '/mock/path'
    police_forces = ['norfolk', 'suffolk']
    crime_data = import_crime_data(base_path, police_forces)

    # Assertions to validate the results
    assert len(crime_data) == 2, "Expected 2 DataFrames in crime_data"
    assert '2021-09-norfolk-street' in crime_data
    assert '2021-09-suffolk-outcomes' in crime_data
    assert crime_data['2021-09-norfolk-street'].equals(pd.DataFrame({'Crime ID': [1, 2]}))
    
    # Ensure the mocks were called as expected
    mock_read_csv.assert_called()
    mock_listdir.assert_any_call(os.path.join(base_path, 'Crime_Dataset'))
    mock_listdir.assert_any_call(os.path.join(base_path, 'Crime_Dataset', '2021-09'))

@mock.patch('os.listdir')
@mock.patch('pandas.read_csv')
@mock.patch('os.path.isdir')
@mock.patch('os.path.exists')
def test_import_crime_data_empty_subfolder(mock_exists, mock_isdir, mock_read_csv, mock_listdir):
    """
    Test case for handling subfolders with no CSV files in 'import_crime_data'.
    Ensures that empty subfolders are skipped without affecting other valid folders.
    """
    # Simulate that the dataset directory exists
    mock_exists.return_value = True
    
    # Simulate directories with one valid folder and one empty folder
    mock_listdir.side_effect = [
        ['2020-01', '2020-02'],        # Root folder contents
        [],                            # 2020-01 is empty
        ['2020-02-suffolk-outcomes.csv']  # 2020-02 contains one valid file
    ]
    mock_isdir.return_value = True
    mock_read_csv.side_effect = [pd.DataFrame({'Crime ID': [3, 4]})]  # For 2020-02 file

    base_path = '/mock/path'
    police_forces = ['norfolk', 'suffolk']
    crime_data = import_crime_data(base_path, police_forces)

    # Assertions
    assert len(crime_data) == 1  # Only the valid file in 2020-02 should be loaded
    assert '2020-02-suffolk-outcomes' in crime_data
    assert '2020-01' not in crime_data  # No data from the empty folder

    # Ensure the mocks were called properly
    mock_listdir.assert_any_call(os.path.join(base_path, 'Crime_Dataset'))
    mock_isdir.assert_called()
    mock_read_csv.assert_called_once()


"""
Testing function 2: 'load_house_price_data'
--------------------------------------------

This test suite ensures that the 'load_house_price_data' function correctly loads house price data from CSV files in the 'House_Price_Dataset' directory. It verifies that the function can handle various scenarios such as missing directories, empty folders, and file loading errors.

Included tests:

1. **Basic File Loading**:
    - Verifies that the function correctly reads multiple CSV files and returns a dictionary with the proper keys and data frames.

2. **Missing Directory**:
    - Ensures the function returns an empty dictionary and logs an error when the directory does not exist.

3. **Empty Directory**:
    - Verifies that the function returns an empty dictionary when no CSV files are present in the directory.

4. **File Load Failure**:
    - Ensures that the function can handle cases where some files fail to load. The test confirms that the function skips these files and logs an error.

These tests cover typical scenarios and edge cases, ensuring that the house price data is correctly loaded and handled by the pipeline.
"""

@mock.patch('os.listdir')
@mock.patch('pandas.read_csv')
@mock.patch('os.path.exists')
def test_load_house_price_data(mock_exists, mock_read_csv, mock_listdir):
    """
    Test case for the 'load_house_price_data' function.
    
    This test verifies the function's ability to load house price data from CSV files and
    handle cases such as missing directories, empty folders, and file loading errors.
    """
    
    # Mock directory existence and file listing
    mock_exists.return_value = True
    mock_listdir.return_value = ['pp-2021.csv', 'pp-monthly-update-new-version.csv']

    # Mock CSV reading to return predefined DataFrames
    mock_read_csv.side_effect = [
        pd.DataFrame({'Price': [250000, 320000]}),  # For 'pp-2021.csv'
        pd.DataFrame({'Price': [270000, 340000]})   # For 'pp-monthly-update-new-version.csv'
    ]

    # Call the function
    base_path = '/mock/path'
    house_price_data = load_house_price_data(base_path)

    # Assertions to validate the results
    assert len(house_price_data) == 2, "Expected 2 DataFrames in house_price_data"
    assert '2021' in house_price_data
    assert 'recent' in house_price_data
    assert house_price_data['2021'].equals(pd.DataFrame({'Price': [250000, 320000]}))
    assert house_price_data['recent'].equals(pd.DataFrame({'Price': [270000, 340000]}))

    # Ensure the mocks were called as expected
    mock_read_csv.assert_called()
    mock_listdir.assert_called_with(os.path.join(base_path, 'House_Price_Dataset'))
    mock_exists.assert_called_with(os.path.join(base_path, 'House_Price_Dataset'))

@mock.patch('os.path.exists')
def test_load_house_price_data_missing_dir(mock_exists):
    """
    Test case for handling missing directory in 'load_house_price_data'.
    
    Ensures that the function returns an empty dictionary and logs an error when
    the directory does not exist.
    """
    mock_exists.return_value = False
    base_path = '/mock/path'
    house_price_data = load_house_price_data(base_path)
    
    assert len(house_price_data) == 0, "Expected an empty dictionary when the directory is missing"

@mock.patch('os.listdir')
@mock.patch('os.path.exists')
def test_load_house_price_data_empty_dir(mock_exists, mock_listdir):
    """
    Test case for handling an empty directory in 'load_house_price_data'.
    
    Ensures that the function returns an empty dictionary when there are no CSV files.
    """
    mock_exists.return_value = True
    mock_listdir.return_value = []  # Simulate empty directory
    
    base_path = '/mock/path'
    house_price_data = load_house_price_data(base_path)
    
    assert len(house_price_data) == 0, "Expected an empty dictionary when no CSV files are present"

@mock.patch('os.listdir')
@mock.patch('pandas.read_csv')
@mock.patch('os.path.exists')
def test_load_house_price_data_file_load_failure(mock_exists, mock_read_csv, mock_listdir):
    """
    Test case for handling a CSV file load failure in 'load_house_price_data'.
    
    Ensures that the function skips files that cannot be read and logs an error.
    """
    # Simulate the existence of the directory and a file listing
    mock_exists.return_value = True
    mock_listdir.return_value = ['pp-2021.csv', 'pp-monthly-update-new-version.csv']

    # Simulate failure on the second file
    mock_read_csv.side_effect = [
        pd.DataFrame({'Price': [250000, 320000]}),  # First file succeeds
        Exception("File load error")                # Second file fails
    ]

    # Call the function
    base_path = '/mock/path'
    house_price_data = load_house_price_data(base_path)

    # Assertions
    assert len(house_price_data) == 1, "Expected 1 DataFrame due to file load failure"
    assert '2021' in house_price_data
    assert 'recent' not in house_price_data

    # Ensure the mocks were called as expected
    mock_read_csv.assert_called()
    mock_listdir.assert_called_with(os.path.join(base_path, 'House_Price_Dataset'))
    mock_exists.assert_called_with(os.path.join(base_path, 'House_Price_Dataset'))


"""
Testing function 3: 'combine_dataframes'
----------------------------------------

This test suite ensures that the 'combine_dataframes' function correctly concatenates crime DataFrames based on 
police force and data type. It verifies that the function handles normal combinations, duplicate keys, and empty DataFrames.

Included tests:

1. **Basic Combination**:
    - Tests combining data frames with no duplicate keys. Verifies correct key extraction and DataFrame merging.

2. **Handling Duplicate Keys**:
    - Ensures data frames with duplicate keys are concatenated properly for the same police force and data type.

3. **Empty DataFrames**:
    - Checks how the function handles empty DataFrames and ensures no errors arise from these cases.

4. **Parameterized Combinations**:
    - Tests various combinations of data frames with varying sizes, using parameterization to ensure robust validation across scenarios.

These tests ensure proper merging, handling of edge cases, and correct concatenation of crime data based on police force and type.
"""


# Fixtures to set up different test cases
@pytest.fixture
def crime_data_basic():
    """Fixture for basic crime data combining test"""
    return {
        '2021-01-norfolk-street': pd.DataFrame({'Crime ID': [1, 2]}),
        '2021-02-suffolk-outcomes': pd.DataFrame({'Crime ID': [3, 4]})
    }

@pytest.fixture
def crime_data_duplicate_keys():
    """Fixture for duplicate keys combining test"""
    return {
        '2021-01-norfolk-street': pd.DataFrame({'Crime ID': [1, 2]}),
        '2021-02-norfolk-street': pd.DataFrame({'Crime ID': [5, 6]}),
        '2021-01-suffolk-outcomes': pd.DataFrame({'Crime ID': [3]}),
        '2021-02-suffolk-outcomes': pd.DataFrame({'Crime ID': [4, 7]})
    }

@pytest.fixture
def crime_data_empty_df():
    """Fixture for testing with an empty dataframe"""
    return {
        '2021-01-norfolk-street': pd.DataFrame(),
        '2021-02-suffolk-outcomes': pd.DataFrame()
    }

# Testing the combine_dataframes function

def test_combine_dataframes_basic(crime_data_basic):
    """
    Test case for combining dataframes correctly when all keys are valid.
    """
    combined_df = combine_dataframes(crime_data_basic)
    
    assert 'norfolk-street' in combined_df
    assert 'suffolk-outcomes' in combined_df
    assert combined_df['norfolk-street'].shape[0] == 2
    assert combined_df['suffolk-outcomes'].shape[0] == 2

def test_combine_dataframes_with_duplicate_keys(crime_data_duplicate_keys):
    """
    Test case for handling duplicate keys by concatenating the dataframes.
    """
    combined_df = combine_dataframes(crime_data_duplicate_keys)
    
    # Check if norfolk-street has been combined correctly
    assert 'norfolk-street' in combined_df
    assert combined_df['norfolk-street'].shape[0] == 4  # 2 rows + 2 rows
    
    # Check if suffolk-outcomes has been combined correctly
    assert 'suffolk-outcomes' in combined_df
    assert combined_df['suffolk-outcomes'].shape[0] == 3  # 1 row + 2 rows

def test_combine_dataframes_with_empty_df(crime_data_empty_df):
    """
    Test case for combining dataframes with empty DataFrames.
    """
    combined_df = combine_dataframes(crime_data_empty_df)
    
    # Check that empty dataframes are handled correctly
    assert 'norfolk-street' in combined_df
    assert combined_df['norfolk-street'].shape[0] == 0
    
    assert 'suffolk-outcomes' in combined_df
    assert combined_df['suffolk-outcomes'].shape[0] == 0

# Parametrized test to check various dataframes
@pytest.mark.parametrize("crime_data, expected_norfolk_rows, expected_suffolk_rows", [
    ({'2021-01-norfolk-street': pd.DataFrame({'Crime ID': [1, 2]}), '2021-02-suffolk-outcomes': pd.DataFrame({'Crime ID': [3, 4]})}, 2, 2),
    ({'2021-01-norfolk-street': pd.DataFrame({'Crime ID': [1, 2]}), '2021-02-suffolk-outcomes': pd.DataFrame()}, 2, 0),
    ({'2021-01-norfolk-street': pd.DataFrame(), '2021-02-suffolk-outcomes': pd.DataFrame()}, 0, 0)
])
def test_combine_dataframes_parametrized(crime_data, expected_norfolk_rows, expected_suffolk_rows):
    """
    Parametrized test case to check combinations of dataframes with varying sizes.
    """
    combined_df = combine_dataframes(crime_data)

    assert 'norfolk-street' in combined_df
    assert 'suffolk-outcomes' in combined_df
    assert combined_df['norfolk-street'].shape[0] == expected_norfolk_rows
    assert combined_df['suffolk-outcomes'].shape[0] == expected_suffolk_rows


"""
Testing function 4: 'check_and_drop_empty_columns'
--------------------------------------------------

This test suite ensures that the 'check_and_drop_empty_columns' function correctly identifies and removes columns 
with all NaN values from the DataFrames in `combined_crime_df`. It also verifies proper error handling and logging.

Included tests:

1. **Normal Case (Columns with NaN)**:
    - Tests a DataFrame with some NaN columns. Expects only non-NaN columns to remain.

2. **No NaN Columns**:
    - Verifies that a DataFrame with no NaN columns remains unchanged.

3. **All NaN Columns**:
    - Ensures that a DataFrame where all columns are NaN results in an empty DataFrame.

4. **Error Case (Non-DataFrame Entry)**:
    - Tests that an error is logged when encountering non-DataFrame entries in the dictionary.

5. **Logging Verification**:
    - Confirms that the log correctly reports the empty columns being dropped.

These tests cover typical scenarios, edge cases, and error handling, ensuring the function behaves as expected 
under all conditions.
"""

# Fixture to create various DataFrames used for tests
@pytest.fixture
def dataframes():
    # DataFrame with some NaN columns
    df_with_nan = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [None, None, None],
        'C': [4, 5, 6]
    })
    
    # DataFrame with no NaN columns
    df_no_nan = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [7, 8, 9],
        'C': [4, 5, 6]
    })
    
    # DataFrame with only NaN columns
    df_all_nan = pd.DataFrame({
        'A': [None, None, None],
        'B': [None, None, None]
    })
    
    return {
        'with_nan': df_with_nan,
        'no_nan': df_no_nan,
        'all_nan': df_all_nan
    }

# Test normal case: DataFrame with NaN columns
@pytest.mark.parametrize("input_df,expected_columns", [
    ("with_nan", ['A', 'C']),  # Expect columns 'A' and 'C' after dropping 'B'
])
def test_check_and_drop_empty_columns_normal_case(input_df, expected_columns, dataframes):
    combined_crime_df = {input_df: dataframes[input_df]}
    
    updated_df = check_and_drop_empty_columns(combined_crime_df)
    
    # Assertions
    assert list(updated_df[input_df].columns) == expected_columns

# Test edge case: DataFrame with no NaN columns
@pytest.mark.parametrize("input_df,expected_columns", [
    ("no_nan", ['A', 'B', 'C'])  # Expect all columns to remain, as none are NaN
])
def test_check_and_drop_empty_columns_no_nan_case(input_df, expected_columns, dataframes):
    combined_crime_df = {input_df: dataframes[input_df]}
    
    updated_df = check_and_drop_empty_columns(combined_crime_df)
    
    # Assertions
    assert list(updated_df[input_df].columns) == expected_columns

# Test edge case: DataFrame with all NaN columns
@pytest.mark.parametrize("input_df,expected_columns", [
    ("all_nan", [])  # Expect no columns after dropping all NaN columns
])
def test_check_and_drop_empty_columns_all_nan_case(input_df, expected_columns, dataframes):
    combined_crime_df = {input_df: dataframes[input_df]}
    
    updated_df = check_and_drop_empty_columns(combined_crime_df)
    
    # Assertions
    assert list(updated_df[input_df].columns) == expected_columns

# Test error case: Non-DataFrame entries in dictionary
def test_check_and_drop_empty_columns_non_dataframe_case(caplog):
    # Simulate a dictionary with a non-DataFrame entry
    combined_crime_df = {
        'invalid_key': 'This is not a DataFrame'
    }
    
    # Run the function with logging
    with caplog.at_level(logging.ERROR):
        updated_df = check_and_drop_empty_columns(combined_crime_df)
        
        # Assertions
        assert 'Expected a DataFrame for key' in caplog.text

# Test logging for successful case: Drop empty columns
def test_check_and_drop_empty_columns_logging(caplog, dataframes):
    combined_crime_df = {'with_nan': dataframes['with_nan']}
    
    # Run the function with logging
    with caplog.at_level(logging.INFO):
        updated_df = check_and_drop_empty_columns(combined_crime_df)
        
        # Check that the appropriate log message has been recorded
        assert "DataFrame 'with_nan' has completely empty columns: B" in caplog.text
        assert "Dropped empty columns from DataFrame 'with_nan'" in caplog.text


"""
Testing function 5: 'remove_duplicates'
---------------------------------------

This test suite verifies that the 'remove_duplicates' function correctly removes duplicate rows from each DataFrame 
in the `combined_crime_df` dictionary. The suite also ensures proper error handling and logging.

Included tests:

1. **Normal Case with Duplicates**: 
    - Confirms duplicate rows are removed from DataFrames. For example, a DataFrame with 5 rows, 2 of which are duplicates, should return 3 rows.

2. **No Duplicates**: 
    - Ensures the function leaves DataFrames unchanged if no duplicates are present.

3. **All Rows Duplicates**: 
    - Verifies that only one row remains if all rows in a DataFrame are duplicates.

4. **Error Handling for Non-DataFrames**: 
    - Ensures the function logs an error if a non-DataFrame object is encountered without crashing.

5. **Logging Verification**: 
    - Checks that the log correctly reports the number of removed rows and the percentage of data reduced.

These tests cover typical use cases, edge cases, and logging to ensure the function behaves correctly and robustly in all scenarios.
"""

# Fixture to create various DataFrames used for tests
@pytest.fixture
def dataframes_with_duplicates():
    # DataFrame with some duplicate rows
    df_with_duplicates = pd.DataFrame({
        'A': [1, 1, 2, 2, 3],
        'B': [4, 4, 5, 5, 6]
    })
    
    # DataFrame with no duplicate rows
    df_no_duplicates = pd.DataFrame({
        'A': [1, 2, 3],
        'B': [4, 5, 6]
    })
    
    # DataFrame where all rows are duplicates
    df_all_duplicates = pd.DataFrame({
        'A': [1, 1, 1],
        'B': [4, 4, 4]
    })
    
    return {
        'with_duplicates': df_with_duplicates,
        'no_duplicates': df_no_duplicates,
        'all_duplicates': df_all_duplicates
    }

# Test normal case: DataFrame with duplicates
@pytest.mark.parametrize("input_df,expected_rows,expected_removed", [
    ("with_duplicates", 3, 2),  # Expect 3 rows after removing 2 duplicates
])
def test_remove_duplicates_normal_case(input_df, expected_rows, expected_removed, dataframes_with_duplicates):
    combined_crime_df = {input_df: dataframes_with_duplicates[input_df]}
    
    updated_df = remove_duplicates(combined_crime_df)
    
    # Assertions
    assert updated_df[input_df].shape[0] == expected_rows
    assert combined_crime_df[input_df].shape[0] == expected_rows

# Test edge case: DataFrame with no duplicates
@pytest.mark.parametrize("input_df,expected_rows,expected_removed", [
    ("no_duplicates", 3, 0),  # No rows should be removed
])
def test_remove_duplicates_no_duplicates(input_df, expected_rows, expected_removed, dataframes_with_duplicates):
    combined_crime_df = {input_df: dataframes_with_duplicates[input_df]}
    
    updated_df = remove_duplicates(combined_crime_df)
    
    # Assertions
    assert updated_df[input_df].shape[0] == expected_rows
    assert combined_crime_df[input_df].shape[0] == expected_rows

# Test edge case: DataFrame with all duplicate rows
@pytest.mark.parametrize("input_df,expected_rows,expected_removed", [
    ("all_duplicates", 1, 2),  # Expect only 1 row after removing 2 duplicates
])
def test_remove_duplicates_all_duplicates(input_df, expected_rows, expected_removed, dataframes_with_duplicates):
    combined_crime_df = {input_df: dataframes_with_duplicates[input_df]}
    
    updated_df = remove_duplicates(combined_crime_df)
    
    # Assertions
    assert updated_df[input_df].shape[0] == expected_rows
    assert combined_crime_df[input_df].shape[0] == expected_rows

# Test error case: Non-DataFrame entries in dictionary
def test_remove_duplicates_non_dataframe_case(caplog):
    # Simulate a dictionary with a non-DataFrame entry
    combined_crime_df = {
        'invalid_key': 'This is not a DataFrame'
    }
    
    # Run the function with logging
    with caplog.at_level(logging.ERROR):
        updated_df = remove_duplicates(combined_crime_df)
        
        # Assertions
        assert 'Expected a DataFrame for key' in caplog.text

# Test logging for successful case: Removing duplicates
def test_remove_duplicates_logging(caplog, dataframes_with_duplicates):
    combined_crime_df = {'with_duplicates': dataframes_with_duplicates['with_duplicates']}
    
    # Run the function with logging
    with caplog.at_level(logging.INFO):
        updated_df = remove_duplicates(combined_crime_df)
        
        # Check that the appropriate log message has been recorded
        assert "with_duplicates: Rows after duplicates removed = 3" in caplog.text
        assert "Rows removed = 2" in caplog.text


"""
Testing function 6: 'clean_text_columns'
----------------------------------------

This test suite validates the functionality of the 'clean_text_columns' function, ensuring that text columns in the crime data are cleaned by converting them to lowercase and removing leading/trailing spaces.

Included tests:

1. **Basic Column Cleaning**:
    - Verifies that specified text columns ('Location', 'Crime type', 'Outcome type', 'Outcome', 'Last outcome category', and 'Object of search') are converted to lowercase and stripped of whitespace.

2. **Handling Missing Columns**:
    - Ensures that the function does not fail when some of the expected columns are missing in the DataFrame.

3. **Non-Text Columns Handling**:
    - Verifies that non-text columns remain unchanged during the cleaning process.

Each test includes assertions to check that the text columns are cleaned appropriately, that the correct columns are processed, and that the structure of the data remains unchanged.
"""

# Fixture to create various DataFrames used for tests
@pytest.fixture
def combined_crime_data():
    """Fixture for providing test cases with different crime data"""
    return {
        'norfolk-street': pd.DataFrame({
            'Location': ['  PARK', 'high street  ', 'CITY '],
            'Crime type': ['  Anti-social behaviour ', 'burglary  ', 'Violence '],
            'Outcome type': ['LOCAL RESOLUTION', '  formal action   ', '   investigation '],
        }),
        'suffolk-outcomes': pd.DataFrame({
            'Location': [' rural area ', '  main road '],
            'Crime type': ['theft', 'criminal damage'],
            'Outcome': ['no further action ', 'Under investigation ']
        })
    }

@pytest.fixture
def incomplete_crime_data():
    """Fixture for providing data where some columns are missing"""
    return {
        'norfolk-street': pd.DataFrame({
            'Crime type': ['Violence', 'Bicycle theft'],
            'Location': ['  CITY', 'small village '],
        }),
        'suffolk-outcomes': pd.DataFrame({
            'Crime type': ['Anti-social behaviour', 'burglary']
        })
    }

# Test normal case: All text columns should be cleaned
def test_clean_text_columns_basic(combined_crime_data):
    cleaned_df = clean_text_columns(combined_crime_data)
    
    # Assertions to check that the text columns are cleaned
    assert all(cleaned_df['norfolk-street']['Location'] == ['park', 'high street', 'city'])
    assert all(cleaned_df['norfolk-street']['Crime type'] == ['anti-social behaviour', 'burglary', 'violence'])
    assert all(cleaned_df['norfolk-street']['Outcome type'] == ['local resolution', 'formal action', 'investigation'])

    assert all(cleaned_df['suffolk-outcomes']['Location'] == ['rural area', 'main road'])
    assert all(cleaned_df['suffolk-outcomes']['Outcome'] == ['no further action', 'under investigation'])

# Test edge case: DataFrames with missing columns
def test_clean_text_columns_missing_columns(incomplete_crime_data):
    cleaned_df = clean_text_columns(incomplete_crime_data)
    
    # Assertions to check that the present text columns are cleaned, while missing columns are ignored
    assert all(cleaned_df['norfolk-street']['Location'] == ['city', 'small village'])
    assert all(cleaned_df['norfolk-street']['Crime type'] == ['violence', 'bicycle theft'])
    
    # Assert that the DataFrame with only 'Crime type' column is unchanged for other columns
    assert all(cleaned_df['suffolk-outcomes']['Crime type'] == ['anti-social behaviour', 'burglary'])

# Test edge case: Ensure non-text columns remain unchanged
@pytest.fixture
def combined_data_with_non_text():
    """Fixture providing DataFrames with non-text columns to ensure they remain unchanged"""
    return {
        'norfolk-street': pd.DataFrame({
            'Location': ['  PARK', 'high street  '],
            'Crime type': ['  Anti-social behaviour ', 'burglary  '],
            'Crime Count': [15, 20]  # Non-text column
        })
    }

def test_clean_text_columns_non_text_columns(combined_data_with_non_text):
    cleaned_df = clean_text_columns(combined_data_with_non_text)
    
    # Assert that text columns are cleaned
    assert all(cleaned_df['norfolk-street']['Location'] == ['park', 'high street'])
    assert all(cleaned_df['norfolk-street']['Crime type'] == ['anti-social behaviour', 'burglary'])
    
    # Assert that the non-text column is unchanged
    assert all(cleaned_df['norfolk-street']['Crime Count'] == [15, 20])


"""
Testing function 39: 'staging_layer'
-------------------------------------

This test suite validates the `staging_layer` function to ensure it performs the full data ingestion, cleaning, transformation, and saving of crime, house price, and postcode data. It includes checks for data quality, missing or incorrect data handling, and proper saving of staged data.

Test coverage includes:

1. **Basic Data Ingestion**:
    - Verifies the function correctly ingests crime, house price, and postcode data from the provided directories.
    
2. **Cleaning and Transformation**:
    - Ensures that data cleaning operations, such as removing duplicates, filling missing values, and dropping unnecessary columns, are applied correctly.
    
3. **Saving Staged Data**:
    - Confirms that the cleaned and transformed datasets are saved as CSV files in the correct directory.

4. **Handling Missing Files**:
    - Tests that the function can handle missing files or directories gracefully.

5. **Error Handling**:
    - Ensures that proper logging and error handling mechanisms are in place when issues arise in the staging process.
"""

# Fixtures for staging layer data
@pytest.fixture
def mock_crime_data():
    return {
        'norfolk-street': pd.DataFrame({'Crime ID': [1, 2]}),
        'suffolk-outcomes': pd.DataFrame({'Crime ID': [3, 4]})
    }

@pytest.fixture
def mock_house_price_data():
    return pd.DataFrame({
        'Transaction unique identifier': ['T1', 'T2'],
        'Price': [200000, 300000],
        'Postcode': ['NR1 1AA', 'IP1 2AB'],
        'Date of Transfer': ['2021-01-01', '2021-02-01'],
        'Property Type': ['D', 'S'],
        'Old/New': ['Y', 'N'],
        'Duration': ['F', 'L']
    })

@pytest.fixture
def mock_postcode_data():
    return pd.DataFrame({
        'Postcode': ['NR1 1AA', 'IP1 2AB'],
        'In Use?': ['Yes', 'Yes'],
        'Latitude': [52.6278, 52.0592],
        'Longitude': [1.2983, 1.1555]
    })

# Mock functions used in the staging layer
@mock.patch('YanYu_Crime_Pipeline_Final.import_crime_data')
@mock.patch('YanYu_Crime_Pipeline_Final.load_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.stage_postcode_data')
@mock.patch('YanYu_Crime_Pipeline_Final.apply_house_price_column_headers')
@mock.patch('YanYu_Crime_Pipeline_Final.combine_dataframes')
@mock.patch('YanYu_Crime_Pipeline_Final.combine_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.check_and_drop_empty_columns')
@mock.patch('YanYu_Crime_Pipeline_Final.remove_duplicates')
@mock.patch('YanYu_Crime_Pipeline_Final.split_date_columns')
@mock.patch('YanYu_Crime_Pipeline_Final.remove_rows_with_missing_data')
def test_staging_layer_ingestion(mock_remove_missing, mock_split, mock_remove_duplicates, mock_drop_empty, 
                                 mock_combine_house, mock_combine, mock_apply_headers, mock_stage_postcode,
                                 mock_load_price, mock_import_crime, mock_crime_data, mock_house_price_data,
                                 mock_postcode_data):
    """
    Test case for the 'staging_layer' function.
    It checks if the data is ingested properly from crime data, house price data, and postcode data.
    """

    # Mocking return values for the functions
    mock_import_crime.return_value = mock_crime_data
    mock_load_price.return_value = mock_house_price_data
    mock_stage_postcode.return_value = mock_postcode_data
    mock_apply_headers.return_value = mock_house_price_data
    mock_combine.return_value = mock_crime_data
    mock_combine_house.return_value = mock_house_price_data
    mock_drop_empty.return_value = mock_crime_data
    mock_remove_duplicates.return_value = mock_crime_data
    mock_split.return_value = mock_crime_data
    mock_remove_missing.return_value = mock_crime_data

    # Call the staging layer function
    base_path = '/mock/path'
    police_forces = ['norfolk', 'suffolk']
    
    staging_layer(base_path, police_forces)

    # Assertions: Verify that the ingestion functions were called correctly
    mock_import_crime.assert_called_with(base_path, police_forces)
    mock_load_price.assert_called_with(base_path)
    mock_stage_postcode.assert_called_with(base_path)

@mock.patch('YanYu_Crime_Pipeline_Final.import_crime_data')
@mock.patch('YanYu_Crime_Pipeline_Final.load_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.stage_postcode_data')
def test_staging_layer_missing_files(mock_stage_postcode, mock_load_price, mock_import_crime):
    """
    Test case for handling missing files in the 'staging_layer' function.
    Ensures that errors are logged and handled when files are missing.
    """
    
    # Simulate an exception during data import
    mock_import_crime.side_effect = FileNotFoundError("Crime data file not found")
    
    base_path = '/mock/path'
    police_forces = ['norfolk', 'suffolk']

    # Run the function and expect a raised exception
    with pytest.raises(FileNotFoundError, match=r".*Crime data file not found*"):
        staging_layer(base_path, police_forces)

@mock.patch('YanYu_Crime_Pipeline_Final.import_crime_data')
@mock.patch('YanYu_Crime_Pipeline_Final.load_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.stage_postcode_data')
@mock.patch('YanYu_Crime_Pipeline_Final.apply_house_price_column_headers')
@mock.patch('YanYu_Crime_Pipeline_Final.combine_dataframes')
@mock.patch('YanYu_Crime_Pipeline_Final.combine_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.check_and_drop_empty_columns')
@mock.patch('YanYu_Crime_Pipeline_Final.remove_duplicates')
@mock.patch('YanYu_Crime_Pipeline_Final.split_date_columns')
@mock.patch('YanYu_Crime_Pipeline_Final.remove_rows_with_missing_data')
def test_staging_layer_cleaning(mock_remove_missing, mock_split, mock_remove_duplicates, mock_drop_empty, 
                                mock_combine_house, mock_combine, mock_apply_headers, mock_stage_postcode,
                                mock_load_price, mock_import_crime, mock_crime_data, mock_house_price_data,
                                mock_postcode_data):
    """
    Test case for checking if the cleaning process is correctly applied during the staging process.
    """

    # Mocking return values for the functions
    mock_import_crime.return_value = mock_crime_data
    mock_load_price.return_value = mock_house_price_data
    mock_stage_postcode.return_value = mock_postcode_data
    mock_apply_headers.return_value = mock_house_price_data
    mock_combine.return_value = mock_crime_data
    mock_combine_house.return_value = mock_house_price_data
    mock_drop_empty.return_value = mock_crime_data
    mock_remove_duplicates.return_value = mock_crime_data
    mock_split.return_value = mock_crime_data
    mock_remove_missing.return_value = mock_crime_data

    # Call the staging layer function
    base_path = '/mock/path'
    police_forces = ['norfolk', 'suffolk']
    
    staging_layer(base_path, police_forces)

    # Assertions: Check that cleaning functions were called in the correct order
    mock_drop_empty.assert_called()
    mock_remove_duplicates.assert_called()
    mock_split.assert_called()
    mock_remove_missing.assert_called()

@mock.patch('YanYu_Crime_Pipeline_Final.import_crime_data')
@mock.patch('YanYu_Crime_Pipeline_Final.load_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.stage_postcode_data')
@mock.patch('YanYu_Crime_Pipeline_Final.apply_house_price_column_headers')
@mock.patch('YanYu_Crime_Pipeline_Final.combine_dataframes')
@mock.patch('YanYu_Crime_Pipeline_Final.combine_house_price_data')
@mock.patch('YanYu_Crime_Pipeline_Final.check_and_drop_empty_columns')
@mock.patch('YanYu_Crime_Pipeline_Final.remove_duplicates')
@mock.patch('YanYu_Crime_Pipeline_Final.split_date_columns')
@mock.patch('YanYu_Crime_Pipeline_Final.remove_rows_with_missing_data')
def test_staging_layer_saving(mock_remove_missing, mock_split, mock_remove_duplicates, mock_drop_empty, 
                              mock_combine_house, mock_combine, mock_apply_headers, mock_stage_postcode,
                              mock_load_price, mock_import_crime, mock_crime_data, mock_house_price_data,
                              mock_postcode_data):
    """
    Test case for verifying the saving process during the staging layer.
    Ensures that cleaned data is correctly saved to CSV files.
    """
    # Mocking return values for the functions
    mock_import_crime.return_value = mock_crime_data
    mock_load_price.return_value = mock_house_price_data
    mock_stage_postcode.return_value = mock_postcode_data
    mock_apply_headers.return_value = mock_house_price_data
    mock_combine.return_value = mock_crime_data
    mock_combine_house.return_value = mock_house_price_data
    mock_drop_empty.return_value = mock_crime_data
    mock_remove_duplicates.return_value = mock_crime_data
    mock_split.return_value = mock_crime_data
    mock_remove_missing.return_value = mock_crime_data

    # Call the staging layer function
    base_path = '/mock/path'
    police_forces = ['norfolk', 'suffolk']
    
    staging_layer(base_path, police_forces)

    # Verify that CSV files were saved
    staged_dir = os.path.join(base_path, 'staged_data')
    assert os.path.exists(staged_dir), "Staged directory should be created"
