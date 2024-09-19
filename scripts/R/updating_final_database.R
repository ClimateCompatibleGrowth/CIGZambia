### This script is for updating the database with newly acquired, cleaned and processed data ###

#Load packages

library(tidyverse)
library(RSQLite)

#####

folder_with_final_databease <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data"  # provide the path where the final database is stored
final_sqlite <- "final_db.sqlite" # this is the name of the final database


## check the tables that exist in database
db_conn_final_db <- DBI::dbConnect(RSQLite::SQLite(),
                                   dbname = paste0(folder_with_final_databease,
                                                   "/", final_sqlite))
db_conn_final_db

lst_final_tbls <- DBI::dbListTables(db_conn_final_db)
lst_final_tbls

## Develop functions to clean and process the newly acquired data

# Imagine for a second that you want to update the "copperbelt_div_elec_substations_data" table with new data
# The new data has fewer variables than the orginal table 
# Variables in new data are 'substation', 'feeder', 'previous_mwh', 'present_mwh', 'consump_mwh', 'md_mw' and 'division'
# The example "new_data_cb_substation.csv" is given in the data folder of the CIGZambia's GitHub repository

df_cb_substations_new <- readr::read_csv("./data/new_data_cb_substation.csv",
                                         show_col_types = FALSE)

df_cb_substations_new


## ---- Two ways to update the database ---- ##

# I recommend the first approach, as it preserves the final database data as is.

## -- 1. Copy all the data in the final database and create a new final database with updated table -- ##
## This function is used to update the existing table in the database
update_existing_db_table_new_database <- function(InputDBWriteFolder,
                                                  InputDBFinal,
                                                  InputDBWrite,
                                                  InputDBTable,
                                                  InputNewData){
  
  db_conn <- dbConnect(SQLite(),
                       paste0(InputDBWriteFolder,
                              "/", InputDBFinal))
  
  db_tbls <- dbListTables(db_conn)
  
  dataVal_all <- NULL
  
  for (db_tbl in db_tbls){
    
    print(db_tbl)  ## print table name
    
    dataVal <- RSQLite::dbReadTable(db_conn, db_tbl)
    
    RSQLite::dbWriteTable(dbConnect(SQLite(),
                                    paste0(InputDBWriteFolder,
                                           "/",
                                           InputDBWrite)),
                          name =  db_tbl,
                          dataVal %>%
                            dplyr::as_tibble(),
                          overwrite = TRUE)
    
    gc()
    
  }
  
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = InputDBTable,
                        InputNewData %>%
                          dplyr::as_tibble(),
                        append = TRUE)
  
}


update_existing_db_table_new_database(folder_with_final_databease,
                                      final_sqlite,
                                      "new_final_db.sqlite",
                                      "copperbelt_div_elec_substations_data",
                                      df_cb_substations_new)


## -- 2. Use the same final database -- ##
## This function is used to update the existing table in the database
update_existing_db_table <- function(InputDBWriteFolder, 
                                     InputDBWrite,
                                     InputDBTable,
                                     InputNewData){
  
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = InputDBTable,
                        InputNewData %>%
                          dplyr::as_tibble(),
                        append = TRUE)
  
}


update_existing_db_table(folder_with_final_databease,
                         final_sqlite,
                         "copperbelt_div_elec_substations_data",
                         df_cb_substations_new)



### ------------------ This is the end of this script ------------------ ####


