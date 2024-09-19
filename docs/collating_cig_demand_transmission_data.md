# collating_cig_demand_transmission_data.R

### **Load Required Packages**

The script begins by loading necessary packages, including `tidyverse` for data manipulation, `zoo` for time series analysis, and `RSQLite` for interacting with SQLite databases.

### **Define Paths**

- It defines paths to important directories:
    - `cig_folder`: Base directory containing the raw data .
    - `transmission_folder`: Subdirectory with the raw connections data .
    - `write_to_db_path`: Directory to store processed data .
    - `datalake_sqlite`: Name of the SQLite database file .
    - `demand_forecast_data_folder`: Full path to the directory with raw data .
    - `trans_raw_data_copy_folder`: Subdirectory containing the .xlsx data files .
    - `trans_data_to_process`: Full path to the directory with .xlsx files .
- **File Conversion Note:** The script notes that the original data files were in .xlsb format and were converted to .xlsx for easier processing in R.

### Function Definitions

The script defines two main functions:

- **`get_raw_transmission_demand_data`**: This function reads data from an .xlsx file.
    - It takes the folder path (`InputFolderPath`), file name (`InputFileName`), and sheet name (`InputNameSheet`) as input.
    - It uses `readxl::read_excel` to read the specified sheet from the .xlsx file and returns the data as a data frame.
- **`process_specific_trans_data`**: This function processes the raw data from a single .xlsx file.
    - It takes the same input arguments as `get_raw_transmission_demand_data`.
    - It calls `get_raw_transmission_demand_data` to read the raw data.

### Data Processing Steps Within `process_specific_trans_data` Function

1. **Initial Data Cleaning**:
    - Selects only columns with at least one non-NA value.
2. **Data Reshaping and Transformation**: The script performs a series of operations to reshape the data, extract relevant information, and prepare it for analysis. These steps are applied to different data categories: MW (Megawatts), MVAr (Megavolt-ampere reactive), MVA (Megavolt-ampere), and kV (kilovolts). Here's a breakdown:
    - **MW Data**:
        - Uses `tidyr::pivot_longer` to convert columns starting with "MW " from wide to long format, creating 'MW_name' and 'MW' columns .
        - Selects and filters relevant columns .
        - Extracts data points and common names from 'MW_name' using `stringr::str_split_fixed`, renames columns, and converts to a tibble.
        - Combines the extracted data with the main dataframe using `dplyr::bind_cols` and removes the 'MW_name' column .
    - **MVAr, MVA, and kV Data**:
        - Similar steps are followed for MVAr, MVA, and kV data, using corresponding column prefixes and names .
3. **Data Merging**:
    - Merges MW and MVAr dataframes (`data_mw` and `data_mvar`) using `dplyr::full_join` based on common columns: Dates, HoD, datapoints, and common_name.
    - Similarly, merges MVA and kV dataframes .
    - Performs a final merge of the combined MW/MVAr and MVA/kV dataframes to create `df_all_data`.
4. **Final Data Cleaning and Type Conversion**:
    - Converts data types of specific columns (`HoD`, `MW`, `MVAr`, `MVA`, `kV`) to numeric .
    - Selects and orders the final columns .
    - The processed data is then returned by the function .

### Data Processing Using Defined Functions

1. **Processing 'Leopards Hill Data.xlsx'**:
    - `process_specific_trans_data` is called with the file path and sheet name to process the "Leopards Hill Data.xlsx" file, and the output is stored in the `df_trans_data` dataframe.
    - The `Sys.time()` function is used to measure the processing time for this file.
2. **Listing Files**:
    - The `list.files` function is used to display all .xlsx files within the specified directory.

### Looping Through Multiple Files

- **`collate_transmission_data` Function**: This function processes all .xlsx files in the given directory .
    - It takes the folder path and sheet name as input.
    - Uses `list.files` to get a list of all .xlsx files .
    - Iterates through each file in the list .
        - Extracts the file name without extensions and cleans it .
        - Calls `process_specific_trans_data` to process each file .
        - Appends the processed data from each file into the `df_merged` dataframe .
    - Returns the final merged data .
- **Executing the Loop**: The `collate_transmission_data` function is called to process all files, and the results are stored in the `df_trans_data_all` dataframe. The processing time is also measured.

### Further Data Manipulation and Extraction

- **Lines Data Extraction**: Extracts data related to lines using string operations and filters based on the presence of 'to' (lowercase) in the 'common_name' column. The results are stored in `df_trans_data_lines`.
- **Lines Data Processing**: Further processes `df_trans_data_lines` to:
    - Extract voltage (kV) and line destination substation information from the 'common_name' column.
    - Create a new dataframe (`df_trans_data_lines2`) by combining `df_trans_data_lines` with the extracted information.

### Direction Data

- Creates a dataframe (`df_trans_direction_data`) containing unique combinations of source file, line destination substation, and voltage from `df_trans_data_lines2` .

### Data Summary

- Generates a timeseries summary (`df_trans_data_summary`) from `df_trans_data_lines2` to determine the maximum and minimum dates for each line and its voltage.

### Summary of the script's achievements

This R script aims to process transmission substation data in Zambia, taking data from multiple .xlsx files, applying a series of transformations and cleaning steps, and preparing it for storage in a datalake database (although the database writing step is commented out in the provided script).

1. **Data Consolidation and Cleaning**: The script successfully reads data from multiple .xlsx files within a specified folder, combines them into a single dataframe (`df_trans_data_all`), and performs initial cleaning, like removing columns with only NA values.
2. **Data Reshaping and Feature Engineering**: The script restructures the data from a wide to long format, making it more suitable for analysis. It extracts key features such as `Dates`, `HoD` (Hour of Day), data points (`datapoints`), common names (`common_name`), and measurement values for megawatts (MW), megavolt-ampere reactive (MVAr), megavolt-ampere (MVA), and kilovolts (kV).
3. **Data Enrichment**: The script further processes the data to extract information on transmission lines, including the destination substation and voltage levels. This information is then combined with the main dataset.
4. **Direction Data Extraction**: The script identifies and isolates data related to the direction of power flow, specifically focusing on lines connecting to substations.
5. **Timeseries Summary**: A summary table (`df_trans_data_summary`) is created to provide insights into the time range of data available for each transmission line and voltage level.

In essence, this script automates the process of taking raw, disparate substation data, cleaning and transforming it, extracting valuable features, and structuring it in a format suitable for further analysis or storage in a database.