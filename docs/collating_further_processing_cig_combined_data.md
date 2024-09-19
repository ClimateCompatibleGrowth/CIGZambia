# Process datalake:

### **Load Required Packages**

- **`tidyverse`**: A collection of R packages designed for data science. It includes packages for data manipulation, visualization, and more.
- **`RSQLite`**: Used to interact with SQLite databases from within R, allowing for data manipulation and storage.

### **Paths and database connections**

It defines the paths to the data lake folder and the names of the relevant SQLite databases:

- `combined_db_folder`: Path to the data lake folder.
- `combined_sqlite`: Name of the initial data lake database.
- `data_lake2_sqlite`: Name of the second data lake database.
- `final_sqlite`: Name of the database where the processed data will be stored.
- **Table Lists:** The script identifies the tables within the initial database and categorizes them into two lists:
    - `clean_tbl_lst`: Tables that require less processing.
    - `not_clean_tbl_lst`: Tables that require more complex processing.

### **Data Processing**

The script utilizes several functions to process data from different tables:

1. **`get_minor_tbl_data_processing` Function:** This function processes tables listed in `clean_tbl_lst`. The function iterates through each table and applies specific data transformations based on the table's name:
    - **`bulk_supply_point_loadings`:**
        - Combines the `hour_start` and `hour_end` columns into a new `hourly_int` column.
        - Selects specific columns for the final output.
    - **`connections`:**
        - No transformations are performed on this table, it is directly written to the new database.
    - **`consumption_all`:**
        - Selects specific columns for the final output.
    - **`substation_map_data`:**
        - No transformations are performed on this table, it is directly written to the new database.
    - **`transmission_electricity_substations_data`:**
        - Renames the "Dates\_char" column to "dates\_char" and "voltage\_kV" to "capacity\_kV".
        - Selects specific columns, including the renamed ones.
        - Arranges the data by `datapoints`, `common_name`, `dates_char`, and `HoD`.
    - **`generation_10_year_inflows_discharge_data`:**
        - Renames the "dates" column to "dates\_char".
        - Selects specific columns, including the renamed one.
        - Arranges the data by `plant_name`.
    - **`generation_10_year_inflows_discharge_generation_data`:**
        - Selects specific columns.
        - Arranges the data by `plant_name`.
    - **`generation_2020_actual_generation_kpi_data`:**
        - Selects specific columns.
        - Arranges the data by `region` and `station`.
    - **`generation_2020_actual_transmission_kpi_data`:**
        - Renames the "voltage" column to "capacity\_volt".
        - Selects specific columns, including the renamed one.
        - Arranges the data by `region` and `capacity_volt`.
    - **`generation_projects_database_gfz_data`:**
        - Removes the "series" column.
        - Arranges the data by `proj_name`.
    - **`generation_zesco_ipp_hourly_data`:**
        - Extracts the numeric hour from `start_hour` and `end_hour` columns and removes the date part.
        - Combines the extracted hours into a new `hourly_int` column.
        - Selects specific columns.
    
    After processing each table, the function writes the transformed data to the second data lake database (`data_lake2_sqlite`).
    
2. **`get_major_tbl_data_processing` Function:** This function processes tables listed in `not_clean_tbl_lst`. These tables primarily contain data related to electricity substations from different regions. The function performs several actions:
    - Filters the `tbl_lst` to include only tables with names containing "\*substations\*".
    - Iterates through the filtered table list and applies region-specific data transformations. For instance:
        - **`copperbelt_electricity_substations_data`:** Renames several columns, adds a 'division' column with the value 'copperbelt,' and stores it in the `df_copperbelt` data frame.
        - Similar processing is done for tables from other regions: 'luapula,' 'ndola,' 'northern,' 'lusaka,' and 'southern'. Each region has its data frame.
    - Combines data from different regions into separate data frames: `df_copperbelt`, `df_northern`, `df_lusaka`, `df_lusaka_loads`, and `df_southern`.
    - Writes each of these regional data frames as separate tables in the `data_lake2_sqlite` database.
3. **`get_major_lm_tbl_data_processing` Function:** This function handles data from the 'luanshya' and 'muchinga' regions. It processes tables:
    - **`luanshya_electricity_substations_data_new_24april` and `muchinga_electricity_substations_data_new_24april`:** Processes these tables to extract and restructure data based on 'readings' which include present, previous, and advance MWh values. It performs the following:
        - Selects and renames columns.
        - Filters out rows with missing 'readings'.
        - Creates new columns to categorize 'readings' as 'pres\_mwh', 'prev\_mwh', and 'adv\_mwh'.
        - Restructures the data using `pivot_wider` to have separate columns for 'main' and 'check' readings of each type ('pres_mwh', 'prev_mwh', 'adv_mwh').
        - Performs multiple joins to combine data based on different keys and columns.
        - Renames columns for clarity.
    
    Finally, it combines the processed data from both 'luanshya' and 'muchinga' into the `df_northern` data frame and writes it to the `data_lake2_sqlite` database as a table named "northern\_2\_div\_elec\_substations\_data".
    

### Data Transfer to Final Database

- **`copy_db_data_to_another_db` Function:** This function transfers all tables from the `data_lake2_sqlite` database to the final database (`final_sqlite`). It does the following:
    - Establishes a connection to `data_lake2_sqlite`.
    - Gets a list of all tables in the database.
    - Iterates through each table, reads its data, and writes it to the `final_sqlite` database.

In summary, the script processes data from multiple tables, cleans it, restructures it according to region-specific logic, and ultimately stores this transformed data in a final SQLite database (`final_sqlite`) for further analysis or use.

### Summary of the script’s achievements

1. **Database Interactions:** The script primarily interacts with three SQLite databases:
    - `combined_data.sqlite`: The input database containing the raw data.
    - `data_lake2.sqlite`: An intermediary database to store processed data.
    - `final_db.sqlite`: The final database holding the cleaned and transformed data, ready for analysis.
2. **Data Processing Functions:** The script utilises three main functions for data processing:
    - `get_minor_tbl_data_processing()`: This function cleans and processes tables with simpler structures. It performs operations like adding columns, selecting specific columns, and minor data transformations.
    - `get_major_tbl_data_processing()`: This function handles tables with more complex structures, specifically those containing data related to electricity substations in different regions of Zambia. It unifies the data from these tables by performing operations like renaming columns for consistency, selecting relevant columns, converting units, and adding a "division" column to indicate the region.
    - `get_major_lm_tbl_data_processing()`: This function is dedicated to processing data from the Luanshya and Muchinga regions. It cleans and restructures the data, notably using a pivoting technique to handle "readings" data effectively.
3. **Data Transfer:** Finally, the `copy_db_data_to_another_db()` function transfers all the processed data from `data_lake2.sqlite` to `final_db.sqlite`.

In essence, the script takes raw electricity data from a data lake, applies various cleaning and transformation steps using custom functions, and outputs a structured and organised dataset ready for further analysis.