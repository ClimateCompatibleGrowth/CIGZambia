
#Load packages
### This script is for collating transmission data from the project folder (E014.3_IRP) to a datalake database ###

## Load packages
library(tidyverse)
library(zoo)
library(RSQLite)


## Point the software where to look (you will have to specify on your computer)
cig_folder <- "~/Documents/Consulting Work/CIG/work/E014.3_IRP" # provide the path where the raw data can stored
transmission_folder <- "2. Implementation/1. Background Information/Demand Forecast Data - Covered by NDA" # this is the folder that has the raw connections data

write_to_db_path <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # this folder where data will be stored (you have to create this folder)
datalake_sqlite <- "combined_data.sqlite" # this is the name of the files (datalake database)

demand_forecast_data_folder <- paste0(cig_folder, '/', transmission_folder)
# '/home/tembo/Documents/Consulting Work/CIG/work/E014.3_IRP/2. Implementation/1. Background Information/Demand Forecast Data - Covered by NDA/'
trans_raw_data_copy_folder <- 'Transmission demand data_copy/Substation EMS Data/' 
## Note, the files with transmission substation data were converted from .xlsb format to .xlsx format. 
# This was done by opening them and then saving them as .xlsx format. It was necessary to make the reading in R easier.
trans_data_to_process <- paste0(demand_forecast_data_folder, '/',trans_raw_data_copy_folder)


## Function for collating the .xlsx version of the transmission substation data demand from folder
get_raw_transmission_demand_data <- function(InputFolderPath, InputFileName, InputNameSheet){
  
  input_xlsx_file <- paste0(InputFolderPath, InputFileName)
  dataVal <- readxl::read_excel(input_xlsx_file,
                                sheet = InputNameSheet)

  gc()
  
  return(dataVal)
  
}


# Function for collating transmission specific demand data from the folder
process_specific_trans_data <- function(InputFolderPath, InputFileName, InputNameSheet){
  
  data_raw <- get_raw_transmission_demand_data(InputFolderPath, InputFileName, InputNameSheet)
  
  data_raw <- data_raw %>%
    dplyr::select(
      where(
        ~sum(!is.na(.x)) > 0
      )
    )

  
  data_mw <- data_raw %>%
    dplyr::filter(!is.na(Dates)) %>% 
    tidyr::pivot_longer(cols = starts_with('MW '),
                        names_to = 'MW_name',
                        values_to = 'MW')
  
  data_mw <- data_mw %>% 
    dplyr::select(Dates, HoD, MW_name, MW) %>% 
    dplyr::filter(!is.na(Dates))
  
  # print(dim(data_mw))
  
  data_keys_mw <- as.data.frame(stringr::str_split_fixed(
    data_mw$MW_name, " XXX ", 3)) %>%
    dplyr::rename(datapoints = V2, common_name = V3) %>%
    dplyr::select(-V1) %>%
    dplyr::as_tibble()

  data_mw <- dplyr::bind_cols(data_mw, data_keys_mw) %>% 
    dplyr::select(-MW_name)
  
  # print(dim(data_mw))
  
  data_mvar <- data_raw %>%
    tidyr::pivot_longer(cols = starts_with('MVAr '),
                        names_to = 'MVAr_name',
                        values_to = 'MVAr')

  data_mvar <- data_mvar %>%
    dplyr::select(Dates, HoD, MVAr_name, MVAr) %>%
    dplyr::filter(!is.na(Dates))
  
  data_keys_mvar <- as.data.frame(stringr::str_split_fixed(
    data_mvar$MVAr_name, " XXX ", 3)) %>%
    dplyr::rename(datapoints = V2, common_name = V3) %>%
    dplyr::select(-V1) %>%
    dplyr::as_tibble()
  
  data_mvar <- dplyr::bind_cols(data_mvar, data_keys_mvar) %>% 
    dplyr::select(-MVAr_name)
  
  # print(dim(data_mvar))
  
  df_mw_mvar <- dplyr::full_join(data_mw, data_mvar,
                                 by = c('Dates', 'HoD',
                                        'datapoints',
                                        'common_name'))

  # print(dim(df_mw_mvar))
  
  data_mva <- data_raw %>%
  tidyr::pivot_longer(cols = starts_with('MVA '),
                      names_to = 'MVA_name',
                      values_to = 'MVA')
  
  data_mva <- data_mva %>%
    dplyr::select(Dates, HoD, MVA_name, MVA) %>%
    dplyr::filter(!is.na(Dates))
  
  data_keys_mva <- as.data.frame(stringr::str_split_fixed(
    data_mva$MVA_name, " XXX ", 3)) %>%
    dplyr::rename(datapoints = V2, common_name = V3) %>%
    dplyr::select(-V1) %>%
    dplyr::as_tibble()
  
  data_mva <- dplyr::bind_cols(data_mva, data_keys_mva) %>% 
    dplyr::select(-MVA_name)
  
  # print(dim(data_mva))
  
  data_kv <- data_raw %>%
    tidyr::pivot_longer(cols = starts_with('kV '),
                        names_to = 'kV_name',
                        values_to = 'kV')
  
  data_kv <- data_kv %>%
    dplyr::select(Dates, HoD, kV_name, kV) %>%
    dplyr::filter(!is.na(Dates))
  
  data_keys_kv <- as.data.frame(stringr::str_split_fixed(
    data_kv$kV_name, " XXX ", 3)) %>%
    dplyr::rename(datapoints = V2, common_name = V3) %>%
    dplyr::select(-V1) %>%
    dplyr::as_tibble()
  
  data_kv <- dplyr::bind_cols(data_kv, data_keys_kv) %>% 
    dplyr::select(-kV_name)
  
  # print(dim(data_kv))
  
  
  df_mva_kv <- dplyr::full_join(data_mva, data_kv,
                                 by = c('Dates', 'HoD',
                                        'datapoints',
                                        'common_name'))
  
  # print(dim(df_mva_kv))
  
  df_all_data <- dplyr::full_join(df_mw_mvar, df_mva_kv,
                                  by = c('Dates', 'HoD',
                                         'datapoints',
                                         'common_name'))
  
  # print(dim(df_all_data))
  
  return(df_all_data %>% 
           dplyr::mutate(HoD = as.numeric(HoD),
                         MW = as.numeric(MW),
                         MVAr = as.numeric(MVAr),
                         MVA = as.numeric(MVA),
                         kV = as.numeric(kV)) %>% 
           # dplyr::arrange(datapoints) %>% 
           dplyr::select(Dates, HoD, datapoints, 
                         common_name, MW, MVAr, MVA, kV)
  )
  
  
}


rm(df_trans_data)
Sys.sleep(1)
gc()


## To get a feel of how the data of a particular substation (Leopards Hill), run the steps below
start_time <- Sys.time()

df_trans_data <- process_specific_trans_data(trans_data_to_process,
                                             "Leopards Hill Data.xlsx",
                                             "Data")

end_time <- Sys.time()
time_needed <- end_time - start_time
print(time_needed)

df_trans_data


## This function list all the files in the folder of interest
list.files(trans_data_to_process,
           pattern = '.xlsx')

## Function for looping through all the .xlsx version of the transmission substation data files
collate_transmission_data <- function(InputFolderPath, InputNameSheet){
  
  file_lst <- list.files(trans_data_to_process,
                         pattern = '.xlsx')
  
  df_merged <- NULL
  
  for(file in file_lst){
    
    print(file)
    
    file_name <- stringr::str_replace(file, ' Data', '')
    file_name <- stringr::str_replace(file_name, '.xlsx', '')
    file_name <- stringr::str_replace(file_name, '.xlsb', '')
    file_name <- stringr::str_trim(file_name)
    
    dataVal = process_specific_trans_data(trans_data_to_process,
                                          file, "Data") %>% 
      dplyr::mutate(source_file = file_name)

    # print(dim(dataVal))

    df_merged <- dplyr::bind_rows(df_merged,
                                  dataVal)
    # 
  }
  
  return(df_merged)
  
}


start_time <- Sys.time() 

df_trans_data_all <- collate_transmission_data(trans_data_to_process, 'Data')

gc()

end_time <- Sys.time()
time_needed <- end_time - start_time
print(time_needed)

df_trans_data_all


## Extract lines data
df_trans_data_lines <- df_trans_data_all %>% 
  dplyr::filter(stringr::str_detect(tolower(common_name), tolower(' to')))

df_trans_data_lines


data_trans_lines <- as.data.frame(stringr::str_split_fixed(
  df_trans_data_lines$common_name, "kV", 2)) %>%
  dplyr::mutate(V2 = stringr::str_replace(V2, 'Line 1', ''),
                V2 = stringr::str_replace(V2, 'Line 2', ''),
                V2 = stringr::str_replace(V2, 'Line 3', ''),
                V2 = stringr::str_replace(V2, ' to ', ''),
                V2 = stringr::str_trim(V2),
                V1 = stringr::str_trim(V1),
                V1 = as.numeric(V1)) %>% 
  dplyr::rename(voltage_kV = V1, line_to_subst = V2) %>%
  # dplyr::select(-V1) %>%
  dplyr::as_tibble()

data_trans_lines

table(data_trans_lines$V2)
length(data_trans_lines$V1)


df_trans_data_lines2 <- dplyr::bind_cols(df_trans_data_lines,
                                         data_trans_lines)

df_trans_data_lines2



## Get direction data (line to a substation)
df_trans_direction_data <- df_trans_data_lines2 %>% 
  dplyr::select(source_file, line_to_subst, voltage_kV) %>% 
  dplyr::distinct()

df_trans_direction_data


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'transmission_electricity_substations_data',
#                       df_trans_data_lines2 %>% 
#                         dplyr::mutate(Dates_char = as.character(Dates)))


max(df_trans_data_lines2$Dates)
min(df_trans_data_lines2$Dates)


## Get a timeseries table summary
df_trans_data_summary <- df_trans_data_lines2 %>%
  # tidyr::pivot_longer(cols = c('jan', 'feb', 'mar', 'apr', 'may', 'jun',
  #                              'jul', 'aug', 'sept', 'oct', 'nov', 'dec'),
  #                     names_to = "month",
  #                     values_to = "units",
  #                     values_drop_na = TRUE) %>%
  # # dplyr::select(msno, file_name, sheet_name) %>%
  dplyr::group_by(line_to_subst, 
                  voltage_kV) %>% #town, month
  dplyr::summarise(max = max(Dates, na.rm = TRUE),
                   min = min(Dates, na.rm = TRUE))

gc()
df_trans_data_summary



### This is the end of this script ####


