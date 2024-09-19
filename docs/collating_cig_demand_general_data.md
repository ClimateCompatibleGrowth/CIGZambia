# collating_cig_demand_general_data.R

### **Load Required Packages**:

- The script starts by loading the `tidyverse`, `zoo`, and `RSQLite` packages for data manipulation and database operations.

### **Define Paths**:

- `bsp_file`: Path to the Excel file containing bulk supply point loadings data.
- `substation_map`: Path to the Excel file containing substation map data.
- `write_to_db_path`: Directory where the combined data will be stored.
- `datalake_sqlite`: Name of the SQLite database file.

### Processing Bulk Supply Point Loadings Data

1. **Data Reading and Initial Cleaning:**
    - The script reads the Excel file containing bulk supply point loadings data using `readxl::read_excel`.
    - It selects only the columns that have at least one non-NA value, indicating that entirely empty columns are removed.
    - A specific column named "...7" is removed. The content and purpose of this column are not specified in the provided script.
2. **Row Filtering and Column Renaming:**
    - Rows containing NA values in the "Hour_End" column are filtered out, ensuring that all remaining rows have a valid hour_end value.
    - All column names are converted to lowercase using the `dplyr::rename_all` function for consistency.
3. **Data Type Conversion and Feature Engineering:**
    - The script converts the data type of the "hour_start" and "hour_end" columns. It extracts the hour component from these columns using `lubridate::hour`.
    - The "date" column is processed to fill missing values. If a date is missing, the last non-missing date value is carried forward to fill the gap. This operation is done using `zoo::na.locf`.
    - A new column "date_char" is created by converting the "date" column to a character format using `as.character`.

### Processing Substation Map Data

1. **Data Reading, Selection, and Transformation:**
    - The Excel file containing the substation map data is read using `readxl::read_excel`.
    - Columns with at least one non-NA value are kept, and those without are discarded.
    - Rows with NA values in the "substation" column are filtered out.
    - All numeric columns are converted to character format using `dplyr::mutate(across(where(is.numeric), as.character))` to ensure consistency in data types.
2. **Data Reshaping:**
    - The data is converted from a wide format to a long format. Columns starting with "xxx " are pivoted to create two new columns: "variable" containing the column name without "xxx " and "datapoints" containing the corresponding values.
    - Rows with NA in the "datapoints" column are removed.
3. **Data Cleaning and Variable Splitting:**
    - The "variable" column, which now holds the names of the original columns, is further processed.
    - The script splits the "variable" column into two columns based on a space delimiter. The first part is stored in the "voltage_kv" column and the second part, if any, is discarded.
    - The "kv" string is removed from the "voltage_kv" column.
4. **Data Exploration:**
    - The script then retrieves unique values and their counts from specific columns like 'substation', 'substation_abbr', and 'voltage_kv' of the processed substation map data to gain quick insights.

### Summary of the script’s achievements

This script prepares data from two Excel files, `2020 Bulk Supply Point Loadings BSP_adjusted.xlsx` and `Substation Data Map_adjusted.xlsx` for storage in a SQLite database, `combined_data.sqlite`.

While the code that would write the data to the database is present, it is commented out, meaning that running the script as-is would only process the data and not store it.

1. **Bulk supply point loadings data:** The script cleans and transforms data from the "Sheet1" sheet of the first file. The operations include selecting relevant columns, removing unnecessary columns, filtering out rows with missing data, standardising the format of column names, extracting hour values from time columns, handling missing date values and converting the date column to the correct datatype.
2. **Substation map data**: The script cleans and transforms data from the "Map" sheet of the second file. Similar to the bulk supply point loadings data, the script selects, renames and filters data columns, removes unnecessary characters, and pivots the data from wide to long format. The script then extracts voltage values from a column containing two pieces of information.

No analysis is performed on either dataset, the script focuses on preparing the data for later use.