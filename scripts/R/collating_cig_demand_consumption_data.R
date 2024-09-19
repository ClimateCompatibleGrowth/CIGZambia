### This script is for collating meter consumption data from the project folder (E014.3_IRP) to a datalake database ###

#Load packages

library(tidyverse)
library(RSQLite)

#####

cig_folder <- "~/Documents/Consulting Work/CIG/work/E014.3_IRP" # provide the path where the raw data can stored
consumptions_folder <- "2. Implementation/1. Background Information/Demand Forecast Data - Covered by NDA" # this is the folder that has the raw connections data

write_to_db_path <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # this folder where data will be stored (you have to create this folder)
datalake_sqlite <- "combined_data.sqlite" # this is the name of the files (datalake database)

## Note the files with consumption data were converted from .xlsb format to .xlsx format. This was done by opening them and then saving them in a different format. It was necessary to make the reading in R easier.


## Function for collating consumption data from the folder
collate_consumption_xlsx_data <- function(FolderPathInput){
  
  dataVal_all <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  for (dir in dirs_list){
    # print(dir) ## Directory name
    
    files <- list.files(path = dir,
                        pattern = '.xlsx') 
    ## Note: the files with consumption data were converted from .xlsb format to .xlsx format. 
    # This was done by opening them and then saving them in a different format. 
    # It was necessary to make the reading in R easier.
    
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
        
        
          dplyr::mutate(TOWN = stringr::str_trim(TOWN),
                        TARIFF = stringr::str_squish(TARIFF),
                        year = stringr::str_replace_all(file_name, pattern = "[[:alpha:]]", ""),
                        year = stringr::str_replace_all(year, pattern = "[[:punct:]]", ""),
                        year = stringr::str_replace_all(year, pattern = "[[:space:]]", ""),
                        year = as.numeric(year)
          ) %>%
          dplyr::rename_all(
            .funs = function(x){
              x %>%
                stringr::str_replace_all(., "UNITS", "") %>%
                stringr::str_replace_all(., pattern = "[[:punct:]]", "") %>%
                stringr::str_replace_all(., pattern = "[[:digit:]]", "")
            }
          ) %>%
          dplyr::rename_all(., .funs = tolower)
        
        gc()
        
        print(head(dataVal))
        # print(colnames(dataVal))
        
        dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
        
      }
      
    }
    
  }
  
  return(dataVal_all)
  
}

df_consumption_xlsx <- collate_consumption_xlsx_data(paste0(cig_folder, '/',
                                                            consumptions_folder,
                                                            '/Consumption data'))

gc()
df_consumption_xlsx


## Get some quick insights 
unique(df_consumption_xlsx$town)
length(unique(df_consumption_xlsx$town))
unique(df_consumption_xlsx$msno)
length(unique(df_consumption_xlsx$msno))
unique(df_consumption_xlsx$tariff)
length(unique(df_consumption_xlsx$tariff))
unique(df_consumption_xlsx$filename)
length(unique(df_consumption_xlsx$filename))
unique(df_consumption_xlsx$sheetname)
length(unique(df_consumption_xlsx$sheetname))
unique(df_consumption_xlsx$year)
length(unique(df_consumption_xlsx$year))


# ### Write the collated data to datalake database
# 
# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'consumption_all',
#                       df_consumption_xlsx)


## Get the summary of the data
df_consumption_data_summary <- df_consumption_xlsx %>%
  tidyr::pivot_longer(cols = c('jan', 'feb', 'mar', 'apr', 'may', 'jun',
                               'jul', 'aug', 'sept', 'oct', 'nov', 'dec'),
                      names_to = "month",
                      values_to = "units",
                      values_drop_na = TRUE) %>%
  # dplyr::select(msno, file_name, sheet_name) %>%
  dplyr::group_by(#tariff, 
                  year) %>% #town, month
  dplyr::summarise(units = sum(units, na.rm = TRUE))

gc()
df_consumption_data_summary


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'consumption_all_summary',
#                       df_consumption_data_summary)



### This is the end of this script ####


