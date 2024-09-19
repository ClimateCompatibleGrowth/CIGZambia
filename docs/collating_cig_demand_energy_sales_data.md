# collating_cig_demand_energy_sales_data.R

### Libraries

- **`tidyverse`**: A collection of R packages for data manipulation and visualization.
- **`zoo`**: Provides functions for working with time series data, including filling missing values.
- **`RSQLite`**: Interface for working with SQLite databases in R.

### Paths and Directories

- **`cig_folder`**: Base directory for raw data.
- **`energy_sales_folder`**: Subdirectory containing energy sales data.
- **`write_to_db_path`**: Directory where the cleaned and combined data will be stored.
- **`datalake_sqlite`**: Name of the SQLite database file that will store the processed data.

## Functions for Data Processing

### `collate_energy_sales_cb_xlsx_data()`

- This function processes data specifically from the Copperbelt Division.
- **Input:** It takes the folder path and the expected number of columns as input.
- **Processing:**
    - It reads all the Excel files within the specified folder.
    - For each sheet within the Excel file:
        - It reads the data while skipping the first row (`skip=0`) and treating all columns as characters.
        - Selects the first 12 columns.
        - Adds new columns for `sheet_name` and `file_name`, providing context to the data.
- **Output:** Returns a dataframe (`dataVal_all`) containing the combined data from all processed sheets and files.

### `collate_energy_sales_lsk_data()`

- This function is tailored to process data specifically from the Lusaka Division.
- **Input:** Takes a folder path as input.
- **Processing:**
    - Similar to the previous function, it reads .xls files from the given directory and iterates through each sheet.
    - Uses conditional statements (`if` and `else if`) to apply specific data cleaning steps based on the file name. For example, it skips the first 8 rows for 'LUSAKA SOUTH.xls', 'LUSAKA EAST.xls', and 'LUSAKA CENTRAL.xls' but skips 9 rows for 'LUSAKA WEST.xls' .
    - Renames specific columns to maintain consistency.
    - Converts all column names to lowercase.
- **Output:** Returns a combined dataframe (`dataVal_all`) containing data from all processed Lusaka Division files.

### `collate_energy_sales_lsk_div_load_data()`

- This function processes data related to the Lusaka Division load.
- **Input:** It accepts a folder path as input.
- **Processing:** Reads data from Excel files within the specified folder and combines them into a single dataframe.
- **Output:** Returns a dataframe (`dataVal_all`) containing data from the processed files.

### `collate_energy_sales_northern_div_data()`

- This function processes data for the Northern Division.
- **Input:** Similar to other functions, it takes a folder path as input.
- **Processing:** Reads data from `.xls` files within the specified folder, combining the data from all files into a single dataframe.
- **Output:** Returns a dataframe (`dataVal_all`) containing data for the Northern Division.

### `collate_eng_sales_lm_special_north_div_data()`

- This function is specifically designed for processing data from Luanshya and Muchinga regions within the Northern Division.
- **Input:** It accepts a folder path and the specific file name as input.
- **Processing:** Reads data from the designated Excel file, processing each sheet.
- **Output:** Returns a dataframe (`dataVal_all`) containing the processed data.

## Dataframes

The script utilises numerous dataframes throughout the data processing pipeline. For brevity, this summary will focus on the most essential dataframes in the script:

- **`df_energy_sales_cb_xlsx`**: Stores the raw data from the Copperbelt Division.
- **`df_energy_sales_cb_xlsx_cleaning`**: This dataframe holds the cleaned data from the Copperbelt Division. The cleaning process involves:
    - Filtering out rows based on values in the 'SERIES' and 'SUBSTATION' columns.
    - Removing unwanted text from the 'sheet_name' column and recoding it to 'sheet_name_recoded'.
    - Adding a 'division' column and populating it with the value 'Copperbelt'.
- **`df_energy_sales_lsk`**: Contains the raw data from the Lusaka Division.
- **`df_energy_sales_lsk_cleaning`**: This dataframe holds the cleaned Lusaka Division data after applying transformations to the 'sheet_name' .
- **`df_energy_sales_lsk2`**: Holds the raw data related to Lusaka Division load.
- **`df_energy_sales_lsk2_cleaning`**: Contains the cleaned Lusaka Division load data. The cleaning process includes:
    - Recoding the 'sheet_name' column for consistency.
    - Extracting the year from the 'sheet_name' and storing it in 'sheet_year_recoded'.
- **`df_energy_sales_lsk2_cleaning_all`**: This dataframe is constructed by combining dataframes from different years (2009 to 2020) after they undergo separate cleaning and transformations. This process involves renaming columns, selecting specific columns, handling missing values, and filtering unwanted data.
- **`df_energy_sales_north`**: Stores the raw data for the Northern Division.
- **`df_energy_sales_luanshya`**: Contains the raw data for the Luanshya region within the Northern Division.
- **`df_energy_sales_muchinga`**: Contains the raw data for the Muchinga region within the Northern Division.
- **`df_energy_sales_northern_xls_cleaning`**: Holds the cleaned data from the "NORTHERN REGIONAL POWER PURCHASES REPORT - JANUARY 2021.xls" file.
- **`df_energy_sales_luapula_xls_cleaning`**: Holds the cleaned data from the "JANUARY 2021 POWER PURCHASES LUAPULA.xls" file.
- **`df_energy_sales_ndola_xls_cleaning`**: Contains the cleaned data from the "NDOLA REGIONAL PURCHASE REPORT JANUARY 2021.xls" file.
- **`df_energy_sales_muchinga_xlsx_cleaning`**: This dataframe holds the cleaned data from the "MUCHINGA REGION - JANUARY 2021 POWER PURCHASES REPORT.xlsx" file.
- **`df_energy_sales_southern_xlsx_cleaning`**: Holds the data from the "BULK REPORT FOR SOUTHERN REGION 2021.xlsx" file.
- **`df_energy_sales_central_xlsx_cleaning`**: Contains the data from the "BULKY CENTRAL REPORT.xlsx" file.
- **`df_energy_sales_eastern_xlsx_cleaning`**: Contains the data from the "EASTERN REGION-SD ENERGY REPORT JANUARY 2021.xls" file.
- **`df_energy_sales_western_xlsx_cleaning`**: Holds the data from the "Western Region Purchases Report for January 2021xlsx.xlsx" file.

## Database Writing (Commented Out)

- The script includes commented-out lines of code that suggest the intention to write the processed dataframes into an SQLite database using the `RSQLite::dbWriteTable` function.

**Note:**  The specific column selections, renaming conventions, and filtering criteria applied in the script highlight its purpose: to extract, clean, and prepare energy sales data for analysis.

## Data Cleaning for Specific Regions

### Lusaka Division (`df_energy_sales_lsk2_cleaning`)

- **Recoding Sheet Names for Consistency:** The script meticulously cleans and recodes the `sheet_name` column to establish a uniform format.
    - For instance, it removes extraneous characters like parentheses, punctuation, and extra spaces. It also replaces abbreviations like "MARCH" with "MAR" and converts numerical month and year representations (like "09" and "10") into "2009" and "2010", respectively.
- **Extracting Years and Filtering by Year:** The script extracts the year from the `sheet_name_recoded` column and stores it in a new column, `sheet_year_recoded`. This step is essential for creating yearly subsets of the data, such as `df_energy_sales_lsk2_cleaning_2009`, `df_energy_sales_lsk2_cleaning_2010`, and so on, up to 2020.
- **Renaming Columns based on Year:** The column renaming process within each yearly subset is noteworthy. The script dynamically adapts to the different column structures present in each year's data. For example, in 2009, columns "...16" and "...19" are renamed to "ACTUAL\_16" and "ACTUAL\_19", and a new 'ACTUAL' column is derived based on their values. In contrast, for 2010 onwards, "...16" and "...17" are renamed to "MVA" and "ACTUAL", respectively.
- **Filtering for 'LSK PEAK DEMAND':** The code snippet `df_load_2012 <- df_energy_sales_lsk2_cleaning_2012 %>% dplyr::filter(SUBSTATION == 'LSK PEAK DEMAND')` suggests that the script might be specifically interested in analysing peak demand data from Lusaka. This highlights the use of filtering to focus on particular data subsets for further analysis.

### Northern Division (`df_northern_cleaned`)

- **Data Combination and Cleaning:** The script combines data from different Northern Division files (including those for Luanshya and Muchinga, which are handled separately) into `df_northern_cleaning`.
- **Creating Voltage Information**: The script creates a new column, `voltage_kV`, by identifying voltage levels mentioned within the `SUBSTATION` column (e.g., '0.4KV', '11kV', '33kV'). If a substation name doesn't include a voltage level, the script sets the `voltage_kV` value to `NA`.
- **String Matching to Remove Irrelevant Rows**: The script makes extensive use of `stringr::str_detect` to identify and remove rows containing irrelevant information based on keywords and phrases. For example:
    - It removes rows where 'SUBSTATION\_2a' contains phrases like "PREVIOUS MONTH," "Points of imports and exports," "ESTIMTED READINGS," "BSPs closer", and many others. This meticulous filtering ensures that the final dataset focuses solely on relevant substation data, eliminating extraneous information.

### Luapula Division (`df_luapula_cleaned`)

- **Recoding and Filtering:** Similar to the Lusaka Division example, the script recodes the `sheet_name` column for consistency and extracts the year into `sheet_year_recoded`. It then filters and renames columns based on patterns observed in the data.
- **Creating Trade Summaries:** The script goes a step further to create separate dataframes like `df_luapula_trade_summary` and `df_luapula_trade_statistics`. These dataframes likely aggregate data related to imports, exports, sales, and losses, indicating an interest in analysing the overall electricity trade in the Luapula region.

### Summary of the script’s achievements

1. **Data Collection:** The script gathers energy sales data from various Excel files (`.xls` and `.xlsx`) organised within folders representing different regions or divisions in Zambia (e.g., Copperbelt, Lusaka, Northern).
2. **Data Cleaning and Transformation:** The script performs extensive cleaning and transformation of the raw data, including:
    - **Standardizing Column Names:** Renames columns from different sources to ensure consistency (e.g., renaming "...4" to "PRESENT_MWh").
    - **Recoding Values:** Cleans and recodes values in columns like `sheet_name` to create more meaningful and standardized data (e.g., converting "JANUARY 2021" to "JAN 2021").
    - **Handling Missing Data:** Employs techniques like last observation carried forward (`zoo::na.locf`) to fill in missing values in a structured manner.
    - **Filtering Irrelevant Data:** Removes rows containing irrelevant information, such as headers, footers, and summary rows within the raw data files.
3. **Data Structuring:** Restructures the data to make it suitable for analysis, for instance:
    - **Extracting Years:** Parses `sheet_name` values to extract the year and stores it in the `sheet_year_recoded` column, enabling time-based analysis.
    - **Creating Regional Identifiers:** Adds columns like `division` and `region` to clearly indicate the geographical origin of each data point.
4. **Data Aggregation:** Combines data from different sources and years into consolidated data frames, such as `df_energy_sales_lsk2_cleaning_all` and `df_southern_div`, which facilitates comprehensive analysis.

In essence, this script automates a complex data processing pipeline to transform raw energy sales data into a clean, structured, and analysis-ready format.