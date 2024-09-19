# collating_cig_demand_connections_data.R

---

### **Load Required Packages**

- `tidyverse`: This is typically used for data wrangling and analysis.
- `RSQLite`: Used for managing SQLite databases within R.

### **Define Paths**

- `cig_folder`: Path to the main project directory.
- `consumptions_folder`: Subdirectory containing raw consumption data (perhaps `.xlsx` files).
- `write_to_db_path`: Directory where processed or summarized data will be written.
- `datalake_sqlite`: The name of the SQLite database file.

### **Define Collation Function with Metadata and Column Name Transformation**

The function **`collate_consumption_xlsx_data(FolderPathInput)`** processes the consumption data by:

- Initializing an empty data frame (`dataVal_all`) to store all data from the files.
- Listing all directories/files in the provided folder path.
- Iterating through each file and its sheets. For each `.xlsx` file:
    - It reads the data from each sheet.
    - Adds metadata: It appends the file name (`filename`) and the sheet name (`sheetname`) as new columns in the data frame.
    - Transforms column names: All column names are converted to lowercase to ensure consistency.
    - **The cleaned data is appended to the combined data frame (`dataVal_all`).**

### **Collate Data**

The script likely calls the `collate_consumption_xlsx_data` function with the folder path that contains the consumption data files, storing the result in `df_consumption_xlsx`.

### **Quick Insights**

After collating the data, some basic insights are extracted, such as unique values or counts for key columns like `town`, `msno`, `tariff`, `filename`, `sheetname`, and `year`.

### **Summarize Data**

The script likely pivots consumption data from a wide format (with months as columns) into a long format to make it easier to aggregate or analyze.

- It then groups the data by `year` to calculate the total consumption for each year, storing the result in `df_consumption_data_summary`.

### **Write to Database (Commented Out)**

The script might include some commented-out lines where the final data (`df_consumption_xlsx` and `df_consumption_data_summary`) can be written to an SQLite database using `dbWriteTable` from the `RSQLite` package. This is likely commented out to give the user flexibility over whether they want to store the results in the database.

### Summary of the script’s achievements

1. **Adding Metadata:** The script adds the file name (file_name) and sheet name (sheet_name) as new columns to each data frame representing a sheet.
2. **Column Name Standardisation**: The code converts all column names to lowercase using dplyr::rename_all(., .funs = tolower), ensuring consistency and avoiding case-sensitivity issues.
3. **Data Combination**: Data from all processed Excel sheets are combined into a single data frame (dataVal_all) using dplyr::bind_rows.
4. Date and Time Handling: The script is equipped to handle date and time columns, as suggested by the commented-out code block. This section uses dplyr::mutate(across(where(is.POSIXt), as. character)) to convert any date or time columns to character format before writing to the SQLite database. While this operation isn't performed in the main collation function, its presence highlights the script's ability to handle temporal data types.