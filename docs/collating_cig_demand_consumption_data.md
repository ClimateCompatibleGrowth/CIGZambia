# collating_cig_demand_consumption_data.R

### **Load Required Packages**

- `tidyverse`: This is typically used for data wrangling and analysis.
- `RSQLite`: Used for managing SQLite databases within R.

### **Define Paths**

- `cig_folder`: Path to the main project directory.
- `consumptions_folder`: Subdirectory containing raw consumption data.
- `write_to_db_path`: Directory where the combined data will be stored.
- `datalake_sqlite`: Name of the SQLite database file.

### **Define Collation Function**

- `collate_consumption_xlsx_data(FolderPathInput)`: This function processes the consumption data from the specified folder.
    - Initializes an empty data frame `dataVal_all`.
    - Lists all directories within the specified path.
    - Iterates through each directory to find `.xlsx` files.
    - For each file, reads the sheet names and processes each sheet by reading its content into a data frame.
    - Cleans and transforms the data:
        - Trims and squishes text columns.
        - Extracts the year from the file name.
        - Renames columns to remove unwanted characters and convert to lowercase.
    - Appends the processed data to `dataVal_all`.
    - Returns the combined data frame.

### **Collate Data**

- Calls the `collate_consumption_xlsx_data` function with the appropriate path to collate the consumption data into `df_consumption_xlsx`.
- Performs garbage collection to free up memory.

### **Quick Insights**

- Extracts unique values and counts for various columns such as `town`, `msno`, `tariff`, `filename`, `sheetname`, and `year`.

### **Summarize Data**

- Pivots monthly consumption columns into a long format.
- Groups by year to calculate the total units consumed per year.
- Stores the summary in `df_consumption_data_summary`.

### **Write to Database (Commented Out)**

- Includes commented-out code for writing both the collated data and the summary data to the SQLite database using `dbWriteTable`.

### Summary of the script’s achievements

1. Data Cleaning and Transformation
    - **String Manipulation:**
        - The script addresses potential inconsistencies in the 'TOWN' column. It employs the `stringr::str_trim` function to remove any leading or trailing white spaces from the town names. This ensures that variations in spacing don't lead to the same town being treated as different entities during analysis. For example, " London " and "London" would be treated as the same entity after this operation.
        - The script also applies the `stringr::str_squish` function to the 'TARIFF' column. This function removes unnecessary spaces within tariff names, standardizing them. For instance, "Tariff A " becomes "Tariff A". This standardization is important for accurate categorization and analysis based on tariffs.
    - **Year Extraction and Transformation:**
        - The script extracts the year from the file names. This assumes that the file names follow a consistent pattern where the year is embedded. The `stringr::str_replace_all` function is used to remove all alphabetic characters, punctuation, and spaces, leaving only the numeric year. For example, a file name like "Consumption_Data_Year2023.xlsx" would be processed to extract "2023".
        - After extraction, the year, initially a character string, is converted to a numeric format using the `as.numeric` function. This data type transformation is crucial for performing numerical operations and comparisons during data analysis.
    - **Column Name Standardization**:
        - The script standardizes all column names, a crucial step for ensuring consistency and facilitating easier data manipulation later in the analysis process. The script achieves this by converting all column names to lowercase using the `tolower` function.
        - Further standardization is achieved by using `stringr::str_replace_all` to remove special characters, digits, and the word "UNITS" from the column names. This ensures that column names are concise, descriptive, and adhere to a common naming convention.
2. Data Aggregation and Summarization
    - **Reshaping data:**
        - The script transforms the structure of the data from a "wide" format to a "long" format, a common operation in data manipulation. This is achieved using the `tidyr::pivot_longer` function. The goal is to make the data more suitable for analysis, especially when dealing with time series or longitudinal data.
        - In the initial "wide" format, each month (January, February, March, etc.) likely has its own column with corresponding consumption values. The `pivot_longer` function consolidates these monthly columns into two new columns:
            - "Month": This column contains the names of the months (e.g., "Jan", "Feb", "Mar").
            - "Units": This column contains the corresponding consumption values for each month.
        - This transformation makes it easier to analyze consumption patterns over time, group data by month, and perform time-series analysis.
    - **Calculating Summary Statistics:**
        - The script focuses on calculating summary statistics, specifically total consumption ("units"), grouped by year. This aggregation helps in understanding overall consumption trends over time.
        - This aggregation is performed using the `dplyr` package. The `group_by` function groups the data by the "Year" column, effectively creating subsets of data for each year. Subsequently, the `summarise` function calculates the sum of "units" (consumption) for each year, providing an aggregated view of yearly consumption.
        - The results are stored in the `df_consumption_data_summary` data frame.