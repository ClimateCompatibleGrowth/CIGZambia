# collating_cig_generation_data.R

### **Load Required Packages**

The script starts by loading necessary R packages:

- `tidyverse`: For data manipulation and visualization.
- `zoo`: For working with time series data.
- `RSQLite`: For interacting with SQLite databases.

### **Define Paths**

Paths for input and output directories are defined:

- `cig_folder`: Path to the main project directory.
- `generation_folder`: Subdirectory containing raw generation data.
- `write_to_generation_db`: Directory where the processed generation data will be stored.
- `write_to_combined_db`: Directory where the combined data will be stored.
- `combined_sqlite`, `generat_sqlite`, `generat_2_sqlite`, `generat_3_sqlite`: Names of the SQLite database files.

### **Data Collation and Organisation**

- The script lists all directories within the specified generation folder. It then uses the `collate_data_by_file_extension` function to identify and organize files with specific extensions (.xlsx, .csv, .xls, .xlsm, .txt, .xlsb).
- **Create a File Inventory:** The script then creates a data frame (`df_cig_gen`) to store information about each file, including its path and extension ().
- **Organise Files by Extension:** Next, the organise_data_by_file_extension_from_main_folder function further organizes the data by file extension. It creates a distinct list of file paths from the `df_cig_gen` data frame and then filters and arranges them based on their extensions, creating the data frame df_cig_gen_folder_organised.

### **Data Processing and Loading**

- **Process Files Based on Extension:** The script uses the process_generation_data_wide function to process the organized data. The script has separate processing blocks for each file extension. This is important because different file types require different methods to read and process their data.
- **Read Data from Files:** For each file extension:
    - The script reads the data from each file.
    - For Excel files (.xlsx, .xls), it loops through each sheet and reads the data.
    - For CSV and TXT files, it reads the entire file at once.
- **Transform Data:** The script performs several data transformations:
    - **Convert to Character:** It converts all data values to character format.
    - **Add Metadata:** It adds metadata columns to the data, including:
        - sheet name (for Excel files).
        - file name.
        - file extension.
        - folder path.
    - **Pivot to Long Format:** It converts wide data (where columns represent variables) to long format (where variables are represented in rows). This makes it easier to analyse the data later.
- **Write Data to Datalake:**
    - The script writes the processed data to an SQLite database (`generation_data.sqlite`, `generation_data_2.sqlite`, `generation_data_3.sqlite`).
    - It creates separate tables for each sheet in Excel files and for each file of other types.

### **Data Merging and Summary**

- **Merge Data from Same Files:** The script merges data from different sheets of the same Excel file into a single table in a new SQLite database (`generation_data_3.sqlite`).
- **Create Summary Table:** It generates a summary table (`df_gen_dim_summary`) that provides an overview of the data in each file, including the number of rows and columns.

### Summary of the script’s achievements

1. **Data Collection:** The script begins by collecting data from files with specific extensions (.xlsx, .csv, .xls, .xlsm, .txt, and .xlsb) located in the designated generation data folder. This is accomplished using the functions `collate_data_by_file_extension` and `organise_data_by_file_extension_from_main_folder`, which systematically scan the folder, identify relevant files based on their extensions, and organise their file paths.
2. **Data Transformation:** Once collected, the data undergoes several transformations:
    - **Character Conversion:** All data values are converted to character format. This ensures uniformity and simplifies subsequent processing.
    - **Metadata Addition:** Metadata, including sheet name, file name, file extension, and folder path, are appended to the data. This provides valuable context and traceability for the data's origin.
    - **Wide to Long Format Conversion:** The `process_generation_data` function transforms data from wide to long format for files with more than three columns. This restructuring is likely done to facilitate further analysis and data manipulation.
3. **Data Storage:**
    - **SQLite Databases:** The script utilizes SQLite databases (generat_2.sqlite, generat_3.sqlite, and generat.sqlite) to store the processed data at different stages. This reflects a structured approach to data handling.
    - **Data Merging:** Data from different sheets of the same Excel file or from tables within a database are merged into single tables. This consolidation ensures that all data belonging to a single source is grouped.
4. **Additional Notes:**
    - **Commented-Out Code:** The script contains commented-out code blocks related to timing operations and writing to a "combined" database. This suggests potential functionalities that might not be fully implemented in the provided excerpt.