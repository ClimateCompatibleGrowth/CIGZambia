# Final database

### **Load Required Packages**

- `tidyverse`: A collection of R packages designed for data manipulation and visualization.
- `RSQLite`: An R package that facilitates interfacing with SQLite databases.

### **Define Paths**

- `folder_with_final_databease`: Path to the directory where the final SQLite database is stored.
- `final_sqlite`: The name of the SQLite database file (`final_db.sqlite`).

### **Data Loading and Preparation**

- The script uses the `readr::read_csv()` function to load the new data from the "new_data_cb_substation.csv" file into a dataframe named `df_cb_substations_new`. The `show_col_types = FALSE` argument suppresses the display of column data types during the import. This new data is intended to be appended to the "copperbelt_div_elec_substations_data" table in the database, although the script does not include code to verify that this table exists in the database.

### **Database Update Functions**

The script defines two functions for updating the database:

1. `update_existing_db_table_new_database**()**`: This function creates a new database file and copies all tables from the original database into the new database. Then, it appends the new data to the specified table within the new database. This method preserves the original database. It carries out the following steps:
    - Establishes a connection to the original database file.
    - Retrieves the list of all tables in the original database.
    - Iterates through each table, reading the data from the original database using `RSQLite::dbReadTable()` into a temporary dataframe `dataVal`.
    - Writes the data from the temporary dataframe to the corresponding table in the new database file using `RSQLite::dbWriteTable()`, overwriting any existing data in the new table.
    - Finally, it writes the new data from `InputNewData` to the specified table (`InputDBTable`) in the new database using `RSQLite::dbWriteTable()`, appending it to the existing data.
2. `update_existing_db_table**()**`: This function directly appends new data to a specified table in the existing database file. It performs the following steps:
    - Establishes a connection to the existing database file.
    - Appends the new data from `InputNewData` to the specified table (`InputDBTable`) in the existing database using `RSQLite::dbWriteTable()`.

### **Database Update Execution**

- The script then calls one of the update functions to execute the database update. The choice of which function to use depends on whether a new database file is desired or if the existing one should be directly modified.
- The script does not explicitly describe any data transformation steps being applied to the data, besides converting the data frame to a tibble using `dplyr::as_tibble()` before writing to the database.

### Summary of the script’s achievements

The key achievement of this script is its ability to add new data to a specific table within the SQLite database (`final_db.sqlite`) while preserving the integrity of the existing data. It achieves this through two primary functions:

- `update_existing_db_table_new_database**()**`: This function creates a new database file (`new_final_db.sqlite`) and copies all tables and data from the original database (`final_db.sqlite`) to the new database. This approach ensures that the original database remains untouched. The function then appends new data, sourced from a data frame (`df_cb_substations_new` in the example), to the targeted table within the new database.
- `update_existing_db_table**()**`: This function offers a more direct approach by appending the new data directly to the designated table in the existing database. This method is potentially faster but carries a higher risk as it directly modifies the original data.

Essentially, the script provides a flexible solution for updating a database. Developers can choose the function that best suits their needs based on their risk tolerance and whether they require a separate, updated database or prefer to modify the existing one directly.