# processing_cig_generation_data.R

### **Load Required Packages**

- `dplyr` : Essential for data manipulation.

### **Define Paths**

- `write_to_generation_db`**:** This path, specified as "/media/tembo/Seagate Portable Drive/Consulting Work/CIG/work/cigzambia_output/data/generation_data"**1**, points to the location of the generation data lake on the user's computer. This is where the generation-related data is stored.
- `write_to_combined_db`**:** Defined as "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data"**1**, this path leads to the folder where the main data lake is stored. This suggests that the processed generation data might be integrated into this main data lake.
- It then defines the file names for the main datalake (`combined_sqlite`) and the generation datalake (`generat_sqlite`).
- Next, the script establishes a connection to the generation datalake database using `DBI::dbConnect`, assigning the connection object to the variable `db_generate`.
- The script then uses `DBI::dbListTables` to retrieve a list of tables present in the `db_generat` database and stores it in the variable `db_generat_tbls`.

### Processing Data from the Generation Datalake

This script processes data related to power generation in Zambia from a SQLite database (`generation_data.sqlite`) and prepares it for storage in a combined data lake (`combined_data.sqlite`). The specific data processing tasks and transformations are outlined below.

### Processing Hourly IPP Data

The script defines a function `get_hourly_ipp_data` to extract and process hourly Independent Power Producer (IPP) data from the `generation_raw_xlsx_data` table in the generation datalake. This function specifically targets data from a file named "ZESCO Gen & IPPs - June 2020 - June 2021-Hourly MW.xlsx".

Here's a breakdown of the function's operations:

1. **Filter and Select Data:**
    - The function filters the `generation_raw_xlsx_data` table to select rows associated with the target file name.
    - It further selects only those columns that have at least one non-missing value, ensuring that only relevant data is carried forward.
2. **Rename Columns for Clarity:**
    - The script renames several columns to enhance readability and understanding. For example:
        - `"...618"` is renamed to `dates`
        - `"KGPS.MW"` is renamed to `kgps_mw`
3. **Select Essential Columns and Filter Rows:**
    - The code selects specific columns relevant for hourly IPP analysis, including `dates`, `start_hour`, `end_hour`, and columns representing power generation from different sources like `kgps_mw`, `kariba_mw`, etc..
    - It then filters the data to retain only rows where both `start_hour` and `end_hour` have valid values.
4. **Handle Missing Dates and Standardize Time:**
    - The script addresses potential inconsistencies and missing values in the `dates` column:
        - It calculates the number of missing date values based on the difference in length between the `sheet_name` and the `dates` column after applying last observation carried forward (`na.locf`).
        - It fills the missing date values by replicating the available dates and then applies `na.locf` to fill any remaining missing values.
    - The `start_hour` and `end_hour` columns are also processed to extract the hour component after being converted to a standard date/time format.
5. **Create a Comprehensive Hourly Time Series:**
    - A sequence of hourly timestamps (`seq_val`) is generated. This sequence covers the entire period from the earliest midnight to the latest 11 PM, based on the unique dates extracted from the IPP data.
6. **Create and Align Time Variables:**
    - A new dataframe, `df_date` is created using the hourly timestamps. This dataframe includes columns for `date_time`, `dates2`, `start_hour2`, and `end_hour2`.
    - The `end_hour2` column is specifically formatted to represent hours in a 0-23 hour format.
7. **Combine and Finalize Data:**
    - The `dataVal` dataframe (containing the IPP data) is combined with `df_date` using a column-wise bind.
    - The `start_hour` and `end_hour` columns in the `dataVal` dataframe are updated with values from `df_date`, ensuring consistency.
    - Finally, redundant columns are removed.

This processed hourly IPP data is stored in the `df_zesco_ipps` dataframe. Although the script includes code to write this dataframe to the combined SQLite database, this code is commented out.

### Processing 10-Year Inflows and Discharge Data

The script processes data from a file named "10 -Year inflow,discharge.xlsx" in two parts: processing summary data and processing the main dataset.

### Processing Summary Data

- The script filters data from the "10 -Year inflow,discharge.xlsx" file for the 'SUMMARY' sheet.
- It selects columns with data and renames them to reflect variables like inflow, outflow, and energy generation for different power stations like Itezhi-Tezhi, Kafue Gorge, Victoria Falls, etc..
- The script then performs a series of transformations:
    1. **Data Cleaning and Preparation:**
    - Filters out rows where 'date_col' is 'Date' or NA to keep only data rows.
    - Uses a predefined list `dates_lst` (not provided in the excerpt) to populate the 'dates' column.
    1. **Restructuring for Analysis:**
    - Selects specific columns containing dates, inflow, outflow, actual energy, potential energy, sheet name, and file name.
    - Pivots the data from wide to long format using `tidyr::pivot_longer`, excluding certain columns from the pivoting operation.
    1. **Extracting Plant Names and Variables:**
    - Creates a `plant_name` column by extracting the plant name from the 'name' column using `stringr::str_detect`. For example, if 'name' contains 'itezhi,' the plant name becomes 'Itezhi-Tezhi'.
    - Extracts plant-specific variables from the 'name' column using `stringr::str_remove` and stores them in `plant_variable`.
    1. **Data Cleaning and Transformation:**
    - Removes the original 'name' column and converts the 'value' column to numeric.
    - Pivots the data back from long to wide format using `tidyr::pivot_wider`, using plant variables for new column names.
    - Arranges the final dataframe by plant name.
- The output of this process is stored in the `df_10_year_inflows_discharge` dataframe. However, this dataframe is removed from memory later in the script.

### Processing the Main 10-Year Dataset

- This part of the script focuses on processing data from all sheets except the 'SUMMARY' sheet in the "10 -Year inflow,discharge.xlsx" file.
- It selects and renames columns to represent months (Jan-Dec), years for each plant ('year_itezhi', 'year_kafue', etc.), and plant names derived from sheet names.
- The script performs several data manipulations:
    1. **Data Cleaning and Transformation:**
        - Converts data in most columns to numeric format.
        - Calculates a common 'year' for each row by taking the maximum value from various 'year_' columns, addressing potential inconsistencies in year representation.
        - Filters out rows with NA or infinite values in the 'year' column.
    2. **Preparing Data for Time Series Analysis:**
        - Selects relevant columns, including the calculated 'year', monthly data, plant name, sheet name, and file name.
        - Appends a 'variable' column containing repeating sequences of "inflow_m3_s", "turbine_discharge_m3_s", and "power_generation_mw", which might correspond to the data structure in the spreadsheet.
- This processed data is stored in the `df_10_year_inflows_discharge_main` dataframe. The script includes commented-out code to save this dataframe to the combined SQLite database, but it is not executed in the provided excerpt.

### Processing 2020 Transmission and Generation KPIs

The script processes data from the "2020 Actual Transmission Generation KPI.xlsx" file, focusing on monthly sheets from January to December. The processing aims to extract and structure KPIs related to power stations and voltage levels.

**1. Data Loading and Filtering:**

- The script first filters data for the relevant file name and sheet names representing months in 2020.

**2. Defining Voltage Types:**

- A list named `voltage_type` is defined to store various voltage levels used in the analysis.

**3. Processing Data for Each Month:**

- The script processes data for each month separately using a similar approach. Below is an example for January:
    - **Data Filtering and Column Renaming:** The script filters the `df_2020_trans_gene_kpis` dataframe to select data for "January 2020" and renames key columns for easier reference.
    - **Processing Station Data:** The code then filters the data for specific stations (e.g., "Kafue Gorge", "Kariba North", etc.) and renames columns to represent availability, planned outages, unplanned outages, and their respective hours for both north and south regions.
    - **Restructuring Data:**
        - The script uses `tidyr::pivot_longer` to transform the data from wide to long format, preparing it for further cleaning and transformation.
        - It cleans the data by filtering out non-data rows, converting values to numeric, and handling missing data using last observation carried forward (`na.locf`).
        - It pivots the data back to wide format using `tidyr::pivot_wider`.
        - The script performs some final cleaning and renaming, such as removing rows with 'TOTALS' and converting availability to a percentage.
    - **Processing Voltage Data:**
        - A similar process is followed for voltage data, where the script filters for voltage levels defined in the `voltage_type` list and restructures the data for each voltage level.
        - This involves the same steps as processing station data, including pivoting, cleaning, and formatting.

**4. Combining Monthly Data:**

- After processing each month's data separately, the script combines the dataframes for stations (`df_2020_trans_gene_kpis_stations_cleaned`) and voltage levels (`df_2020_trans_gene_kpis_volts_cleaned`) using `dplyr::bind_rows`.
- The script includes commented-out code that suggests these cleaned dataframes would be written to the combined SQLite database, but this code is not executed in the provided excerpt.

### Processing Generation Projects Database

The script processes data from a file named "Generation-Projects-Database-GFZ-27042020 (1).xlsx" to extract and clean information about generation projects.

1. **Data Loading and Column Transformation:**
- The script loads data from the specified file and selects relevant columns, renaming them to more descriptive names.
- It then applies a custom function `change_ticks` to several 'status' columns, converting symbols like '√√', '√', 'xxx', etc., into more descriptive text.
1. **Applying a Custom Collation Function:**
- The script defines a custom function `change_ticks` to handle specific symbols found in the data and map them to meaningful categories.
- The function uses a series of `ifelse` statements to match and replace different symbols with corresponding textual descriptions.
    - For example, '√√' is replaced with 'Completed', '√' with 'On-going or preparedy', and so on.
- This function is specifically applied to columns containing project status information ('status_pre_feas', 'status_feas', 'status_committed', 'status_candidate') to make the data more interpretable.
1. **Final Dataframe Creation:**
- The processed data, including the transformed status columns, is stored in the `df_gfz_projects_database` dataframe.
- The script includes commented-out code indicating that this dataframe is intended to be written to the combined SQLite database, but this operation is not performed in the provided excerpt.

### Summary of the script's achievements

1. **Creation of a Unified Datalake:** The script connects to two SQLite databases: a "generation datalake" (`generation_data.sqlite`) containing raw data and a "combined datalake" (`combined_data.sqlite`) intended to store processed data. This setup suggests an effort to consolidate data from different sources into a more accessible and analyzable format.
2. **Processing of Diverse Generation Data:** The script processes data from at least three different Excel files, each with its own structure and content. This highlights the script's ability to handle a variety of data sources commonly encountered in real-world scenarios.
3. **Extraction and Transformation of Hourly IPP Data:** The script successfully extracts hourly generation data for Independent Power Producers (IPPs) from the file "ZESCO Gen & IPPs - June 2020 - June 2021-Hourly MW.xlsx". It addresses issues like missing dates, standardizes time formats, and creates a comprehensive hourly time series of generation data.
4. **Analysis of 10-Year Hydrological Data:** The script processes data from "10 -Year inflow,discharge.xlsx", extracting both summary statistics and detailed monthly data on inflows, turbine discharge, and power generation for various hydroelectric plants. It restructures the data to facilitate time series analysis and comparison across different plants.
5. **Organization of Transmission and Generation KPIs:** The script processes data from "2020 Actual Transmission Generation KPI.xlsx", focusing on monthly key performance indicators (KPIs). It extracts data on station availability and outages, as well as transmission line availability for different voltage levels. It cleans, aggregates, and structures the data to enable monthly performance monitoring and analysis.
6. **Cleaning and Categorization of Generation Projects Database:** The script processes data from "Generation-Projects-Database-GFZ-27042020 (1).xlsx", cleaning and categorizing information about various generation projects. It uses a custom function (`change_ticks`) to translate symbolic representations of project status into meaningful text labels, improving the data's interpretability.
7. **Data Preparation for Database Storage:** Throughout the script, there are several instances of commented-out code blocks containing the function `RSQLite::dbWriteTable`. If these blocks were uncommented and executed, they would write the processed dataframes to the combined SQLite database. This suggests that the script's ultimate goal is to populate the combined datalake with cleaned and prepared data, but this step is not fully implemented in the provided excerpt.