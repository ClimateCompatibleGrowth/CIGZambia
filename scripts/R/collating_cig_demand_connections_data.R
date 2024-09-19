### This script is for collating connections data from the project folder (E014.3_IRP) to a datalake database ###

## Load packages
library(tidyverse)
library(zoo)
library(RSQLite)


## Point the software where to look (you will have to specify on your computer)
cig_folder <- "~/Documents/Consulting Work/CIG/work/E014.3_IRP" # provide the path where the raw data is stored
connections_folder <- "2. Implementation/1. Background Information/Demand Forecast Data - Covered by NDA" # this is the folder that has the raw connections data

write_to_db_path <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # this folder where data will be stored (you have to create this folder)
datalake_sqlite <- "combined_data.sqlite" # this is the name of the files (datalake database)


## Function for collating connections data from the folder
collate_connections_xlsx_data <- function(FolderPathInput){

  dataVal_all <- NULL

  dirs_list <- list.dirs(path = FolderPathInput)

  for (dir in dirs_list){
    # print(dir) ## Directory name

    files <- list.files(path = dir,
                        pattern = '.xlsx')
    # print(files)


    for (file in files){
      print(file)  ## File name

      file_path <- paste0(dir, "/", file)
      print(file_path)

      print(readxl::excel_sheets(path = file_path))

      sheet_names <- readxl::excel_sheets(path = file_path)

      for (sheet_name in sheet_names){

        print(sheet_name)

        dataVal <- readxl::read_excel(file_path,
                                      sheet = sheet_name,
                                      skip = 0) %>%
          dplyr::mutate(file_name = file,
                        sheet_name = sheet_name) %>%
          dplyr::rename_all(., .funs = tolower)

        print(colnames(dataVal))

        dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)

        }

    }

  }

  return(dataVal_all)

}

df_connections_xlsx <- collate_connections_xlsx_data(paste0(cig_folder, '/',
                                                            connections_folder,
                                                            '/Connections'))

gc()
df_connections_xlsx


## Get some quick insights 
unique(df_connections_xlsx$csc)
length(unique(df_connections_xlsx$csc))
table(df_connections_xlsx$csc, df_connections_xlsx$status)
table(df_connections_xlsx$csc, df_connections_xlsx$type_of_account)
table(df_connections_xlsx$type_of_account)
max(df_connections_xlsx$status_date)
min(df_connections_xlsx$status_date)
max(df_connections_xlsx$application_date)
min(df_connections_xlsx$application_date)
max(df_connections_xlsx$quotation_prepared_date)
min(df_connections_xlsx$quotation_prepared_date)



### Write the collated data to datalake database

RSQLite::dbWriteTable(dbConnect(SQLite(),
                                paste0(write_to_db_path,
                                       "/",
                                       datalake_sqlite)),
                      name =  'connections',
                      df_connections_xlsx %>%
                        dplyr::mutate(across(where(is.POSIXt),
                                             as.character)))

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'connections',
#                       df_connections_xlsx %>%
#                         dplyr::mutate(across(where(is.POSIXt),
#                                              as.character)))

### This is the end of this script ####


