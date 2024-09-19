# collating_lcms2015_data_for_cig.R

This R script extracts and processes data from the 2015 Zambia Labour Force and Migration Survey (LCMS) for analysis related to Gender and Social Inclusion. 

### **Load Required Packages**

- `tidyverse`: This is typically used for data wrangling and analysis.
- `RSQLite`: Used for managing SQLite databases within R.

### **Define Paths**

- `write_to_data_lake2_db`: Directory where the combined data will be stored.
- `combined_sqlite`: Name of the combined data SQLite database file.
- `data_lake2_sqlite`: Name of another SQLite database file.
- `lcms_2015_folder`: Path to the LCMS databases.
- `person_db`: Name of the LCMS household member database.
- `hhold_db`: Name of the LCMS household database.

### **Databases and connections**

- `person_db`: LCMS household member database (`lcms2015_sectionsPerson.db`)
- `hhold_db`: LCMS household database (`lcms2015_sectionHousehold.db`)
- `combined_sqlite`: Name for a data lake database file (`combined_data.sqlite`)
- `data_lake2_sqlite`: Another name for a data lake database file (`data_lake2.sqlite`)
- `db_conn_pers`: Connection to the `person_db` database using `DBI::dbConnect` and specifying the SQLite driver.
- `db_conn_hh`: Connection to the `hhold_db` database using `DBI::dbConnect` and specifying the SQLite driver.

### **Collate Data**

- **General Data:**
    - Extracts and selects specific columns from the 'general' table in `person_db` and converts it to a tibble, storing it in `df_general`.
- **Section 1 Data:**
    - Extracts, mutates, and selects specific columns from the 'section1' table in `person_db`, adjusting `sec1q7` values, and converts it to a tibble, storing it in `df_sec1`.
- **Section 2 Data:**
    - Extracts all columns from the 'section2' table in `person_db` and converts it to a tibble, storing it in `df_sec2`.
- **Section 4 Data:**
    - Extracts and selects specific columns from the 'section4' table in `person_db` and converts it to a tibble, storing it in `df_sec4`.
- **Section 5 Data:**
    - Extracts and selects specific columns from the 'section5' table in `person_db` and converts it to a tibble, storing it in `df_sec5`.
- **CSO 2015 Poverty Analysis Data:**
    - Extracts and selects specific columns from the 'CSO 2015 Poverty Analysis' table in `hhold_db` and converts it to a tibble, storing it in `df_pa`.

### **Write to Database (Commented Out):**

- Includes commented-out code for writing the collated data (`df_general`, `df_sec1`, `df_sec2`, `df_sec4`, `df_sec5`, `df_pa`) to the SQLite database using `RSQLite::dbWriteTable`.

### Summary of the script’s achievements

1. **Extracts data** from **seven tables** across the `lcms2015_sectionsPerson.db` and `lcms2015_sectionHousehold.db` databases.
2. **Selects relevant columns** based on the needs of the intended Gender and Social Inclusion analysis.
3. **Cleans variables**, such as creating a new variable (`sec1q7_adj`) from two existing variables in the `section1` table by:
    - Adding their numeric values.
    - Replacing any 0 values with `NA`.
4. **Renames some variables** for clarity, such as renaming `prov` to `province`.
5. **Saves the cleaned and processed data** into **six separate dataframes**:
    - `df_general`: Contains general information about individuals.
    - `df_sec1`: Contains data from Section 1 of the survey at the individual level.
    - `df_sec2`: Contains data from Section 2 of the survey at the individual level.
    - `df_sec4`: Contains data from Section 4 of the survey at the individual level.
    - `df_sec5`: Contains data from Section 5 of the survey at the individual level.
    - `df_pa`: Contains poverty analysis data at the household level.