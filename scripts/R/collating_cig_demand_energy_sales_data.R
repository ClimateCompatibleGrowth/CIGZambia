### This script is for collating distribution data (energy sales data) from the project folder (E014.3_IRP) to a datalake database ###

#Load packages
library(tidyverse)
library(zoo)
library(RSQLite)


## Point the software where to look (you will have to specify on your computer)
cig_folder <- "~/Documents/Consulting Work/CIG/work/E014.3_IRP" # provide the path where the raw data can stored
energy_sales_folder <- "2. Implementation/1. Background Information/Demand Forecast Data - Covered by NDA" # this is the folder that has the raw connections data

write_to_db_path <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # this folder where data will be stored (you have to create this folder)
datalake_sqlite <- "combined_data.sqlite" # this is the name of the files (datalake database)


# Function for collating energy consumption data (copperbelt division) from the folder
collate_energy_sales_cb_xlsx_data <- function(FolderPathInput, InputDim){
  
  dataVal_all <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  for (dir in dirs_list){
    # print(dir) ## Directory name
    
    files <- list.files(path = dir,
                        pattern = '.xlsx')
    print(files)
    
    
    for (file in files){
      print(file)  ## File name

      file_path <- paste0(dir, "/", file)
      print(file_path)

      # print(readxl::excel_sheets(path = file_path))

      sheet_names <- readxl::excel_sheets(path = file_path)
      print(sheet_names)

      for (sheet_name in sheet_names){

        print(sheet_name)

        dataVal <- readxl::read_excel(file_path,
                                      sheet = sheet_name,
                                      skip = 0,
                                      col_names = FALSE) %>%
          dplyr::mutate_all(as.character) %>% 
          dplyr::select(1:12) %>% 
          dplyr::mutate(sheet_name = sheet_name,
                        file_name = file)
        # 
        print(head(dataVal))
        print(dim(dataVal))
        print(dataVal[4, ])
        
        dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
        gc()

      }

    }
    
  }
  gc()
  return(dataVal_all)
  
}

df_energy_sales_cb_xlsx <- collate_energy_sales_cb_xlsx_data(paste0(cig_folder, '/',
                                                                    energy_sales_folder, 
                                                                    '/Energy sales/Copperbelt Division'),
                                                             12)
gc()
df_energy_sales_cb_xlsx


table(df_energy_sales_cb_xlsx$...1)


df_energy_sales_cb_xlsx_cleaning <- df_energy_sales_cb_xlsx %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...2', FEEDER = '...3', 
                SERIES = '...1', MAIN_PRESENT_MWh = '...5',
                MAIN_PREVIOUS_MWh = '...4', CHECK_PRESENT_MWh = '...7',
                CHECK_PREVIOUS_MWh = '...6', MAIN_ADVANCE = '...8',
                CHECK_ADVANCE = '...9', VARIANCE = '...10',
                VARIANCE_SHARE = '...11', MD_MW = '...12') %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(is.na(SERIES) & is.na(SUBSTATION),
                                NA, ifelse(is.na(SUBSTATION),
                                           SERIES,
                                           SUBSTATION)),
                SUBSTATION_2 = zoo::na.locf(SUBSTATION_2)
                ) %>% 
  dplyr::filter(!SERIES %in% c('SUBSTATION', 'ZESCO LIMITED',
                               'ZESCO LIMITED .     COPPERBELT DIVISION',
                               'POWER PURCHASES',
                               'MONTHLY LOAD FIGURES (MWh)'),
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !stringr::str_detect(tolower(SERIES), tolower('Date prepared'))
                ) %>% 
  dplyr::mutate(sheet_name_recoded = stringr::str_trim(sheet_name),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded), 
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2020 CORRECTED", 
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "AMENEDED", 
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "DIVISIONAL REVISED", 
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "DIVISION", 
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                division = 'Copperbelt')
  

df_energy_sales_cb_xlsx_cleaning

unique(df_energy_sales_cb_xlsx_cleaning$sheet_name)
unique(df_energy_sales_cb_xlsx_cleaning$sheet_name_recoded)

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'copperbelt_electricity_substation_statistics',
#                       df_energy_sales_cb_xlsx_cleaning)


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'copperbelt_electricity_substations_data',
#                       df_energy_sales_cb_xlsx_cleaning)


# Function for collating energy consumption data (lusaka division) from the folder
collate_energy_sales_lsk_data <- function(FolderPathInput){
  
  dataVal_all <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  for (dir in dirs_list){
    # print(dir) ## Directory name
    
    files <- list.files(path = dir,
                        pattern = '.xls')
    # print(files)
    
    
    for (file in files){
      # print(file)  ## File name
      
      file_path <- paste0(dir, "/", file)
      # print(file_path)
      
      sheet_names <- readxl::excel_sheets(path = file_path)
      # print(sheet_names)
      sheet_names <- sheet_names[!sheet_names %in% 
                                   c("Sheet1", "Sheet2")] #, "MAY CORRECTED"
      
      for (sheet_name in sheet_names){
        
        # print(sheet_name)
        
        if(file == "LUSAKA SOUTH.xls"){
          print(file_path)
          print(file)
          print(sheet_name)

          dataVal <- readxl::read_excel(file_path,
                                        sheet = sheet_name,
                                        skip = 8,
                                        col_names = TRUE) %>%
            dplyr::mutate_all(as.character) %>%
            dplyr::mutate(sheet_name = sheet_name,
                          file_name = file) %>%
            dplyr::select(1:7, sheet_name, file_name) %>% 
            dplyr::rename(SUBSTATION = 1, UNIT = 2, PREVIOUS_kWh = 3,
                          PRESENT_kWh = 4, CONSUMPTION_kWh = 5, 
                          KVA = 6, REMARKS = 7) %>% 
            dplyr::rename_all(., .funs = tolower)
          #
          print(head(dataVal))
          print(dim(dataVal))
          # print(dataVal[4, ])

          dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
          gc()
          
          } else if(file == "LUSAKA EAST.xls"){
          print(file_path)
          print(file)
          print(sheet_name)

          dataVal <- readxl::read_excel(file_path,
                                        sheet = sheet_name,
                                        skip = 8,
                                        col_names = TRUE) %>%
            dplyr::mutate_all(as.character) %>%
            dplyr::mutate(sheet_name = sheet_name,
                          file_name = file) %>%
          #  dplyr::rename(CONSUMPTION = 5) %>%
            dplyr::select(1:7, sheet_name, file_name) %>% 
            dplyr::rename(SUBSTATION = 1, UNIT = 2, PREVIOUS_kWh = 3,
                          PRESENT_kWh = 4, CONSUMPTION_kWh = 5,
                          KVA = 6, REMARKS = 7) %>% 
            dplyr::rename_all(., .funs = tolower)
          #
          print(head(dataVal))
          print(dim(dataVal))
          # print(dataVal[4, ])

          dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
          gc()
          
        } else if(file == "LUSAKA CENTRAL.xls"){
          print(file_path)
          print(file)
          print(sheet_name)

          dataVal <- readxl::read_excel(file_path,
                                        sheet = sheet_name,
                                        skip = 8,
                                        col_names = TRUE) %>%
            dplyr::mutate_all(as.character) %>%
            dplyr::mutate(sheet_name = sheet_name,
                          file_name = file) %>%
            dplyr::select(1:7, sheet_name, file_name) %>% 
            dplyr::rename(SUBSTATION = 1, UNIT = 2, PREVIOUS_kWh = 3,
                          PRESENT_kWh = 4, CONSUMPTION_kWh = 5, 
                          KVA = 6, REMARKS = 7) %>% 
            dplyr::rename_all(., .funs = tolower)
          #
          print(head(dataVal))
          print(dim(dataVal))
          # print(dataVal[4, ])

          dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
          gc()
          
          } else if(file == "LUSAKA WEST.xls"){
          print(file_path)
          print(file)
          print(sheet_name)

          dataVal <- readxl::read_excel(file_path,
                                        sheet = sheet_name,
                                        skip = 9,
                                        col_names = TRUE) %>%
            dplyr::mutate_all(as.character) %>%
            dplyr::mutate(sheet_name = sheet_name,
                          file_name = file) %>%
            # dplyr::rename(CONSUMPTION = 5) %>%
            dplyr::select(1:7, sheet_name, file_name) %>% 
            dplyr::rename(SUBSTATION = 1, UNIT = 2, PREVIOUS_kWh = 3,
                          PRESENT_kWh = 4, CONSUMPTION_kWh = 5,
                          KVA = 6, REMARKS = 7) %>% 
            dplyr::rename_all(., .funs = tolower)
          #
          print(head(dataVal))
          print(dim(dataVal))

          dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
          gc()
          
        } else{
          
          print(paste0("This file '", file, "' has not yet been programmed."))
          
        }
        
      }
      
    }
    
  }
  gc()
  return(dataVal_all)
  
}

df_energy_sales_lsk <- collate_energy_sales_lsk_data(paste0(cig_folder, '/',
                                                            energy_sales_folder, 
                                                            '/Energy sales/Lusaka Division'))
gc()
df_energy_sales_lsk

unique(df_energy_sales_lsk$sheet_name)


df_energy_sales_lsk_cleaning <- df_energy_sales_lsk %>% 
  dplyr::mutate(sheet_name_recoded = stringr::str_trim(sheet_name),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "new", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded), 
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = " 12", "2012"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "E20", " 20"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "E 20", " 20"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "Y20", "Y 20"),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2020 CORRECTED", 
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "[[:punct:]]", " "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = ifelse((file_name == "LUSAKA EAST.xls" & 
                                               sheet_name == "MAY CORRECTED") |
                                              (file_name == "LUSAKA SOUTH.xls" & 
                                                 sheet_name == "MAY CORRECTED"),
                                            "MAY 2020", sheet_name_recoded),
                
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded)
  )

df_energy_sales_lsk_cleaning


# # RSQLite::dbWriteTable(dbConnect(SQLite(),
# #                                 paste0(write_to_db_path,
# #                                        "/",
# #                                        datalake_sqlite)),
# #                       name =  'lusaka_electricity_trade_summary',
# #                       df_lsk_trade_summary)
# 

# 
# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'lusaka_electricity_substation_statistics',
#                       df_lsk_trade_substation_cleaning)



df_lsk_cleaned <- df_energy_sales_lsk_cleaning %>% 
  dplyr::mutate(substation_2 = ifelse(!tolower(substation) %in% 
                                        tolower(c('0.4KV', '11kV', '33kV')),
                                      substation, NA),
                substation_2 = ifelse(substation_2 == ';',
                                      NA, substation_2),
                volt_kV1 = ifelse(tolower(substation) %in% 
                                    tolower(c('0.4KV', '11kV', '11KV', '33kV')),
                                  substation, NA),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower('220/33KV')),
                                  '220/33kV', unit),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower('132/33kV')),
                                  '132/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower('88/ 33kV')),
                                  '88/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower('88/33kV')),
                                  '88/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower('88/33 kV')),
                                  '88/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower('88/11kV')),
                                  '88/11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower(' 33kV')),
                                  '33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower(' 11kV')),
                                  '11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(unit), tolower(' 0.4kV')),
                                  '0.4kV', volt_kV2),
                volt_kV2 = ifelse(tolower(volt_kV2) %in% 
                                    tolower(c('0.4KV', '11kV', '11KV', '33kV',
                                              '220/33kV', '132/33kV',
                                              '88/33kV', '88/11kV')),
                                  volt_kV2, NA),
                voltage_kV = ifelse(is.na(volt_kV1) & is.na(volt_kV2),
                                    NA, 
                                    ifelse(is.na(volt_kV1),
                                           volt_kV2,
                                           volt_kV1)),
                
                substation_name = c(NA, na.locf(as.zoo(substation_2))),
                division = 'Lusaka'
  )


df_lsk_cleaned


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'lusaka_electricity_substations_data',
#                       df_lsk_cleaned)



# Function for collating energy consumption data (lusaka division load) from the folder
collate_energy_sales_lsk_div_load_data <- function(FolderPathInput){
  
  dataVal_all <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  for (dir in dirs_list){
    # print(dir) ## Directory name
    
    files <- list.files(path = dir,
                        pattern = '.xls')
    # print(files)
    
    
    for (file in files){
      # print(file)  ## File name
      
      file_path <- paste0(dir, "/", file)
      # print(file_path)
      
      sheet_names <- readxl::excel_sheets(path = file_path)
      
      sheet_names <- sheet_names[!sheet_names %in% 
                                   c("Sheet1", "Sheet2")] 
      
      for (sheet_name in sheet_names){
        
        # print(sheet_name)
        
        if(file == "LUSAKA DIVISION LOAD.xls"){
          print(file_path)
          print(file)
          print(sheet_name)
          
          dataVal <- readxl::read_excel(file_path,
                                        sheet = sheet_name,
                                        skip = 0,
                                        col_names = FALSE) %>%
            dplyr::mutate_all(as.character) %>%
            dplyr::mutate(sheet_name = sheet_name,
                          file_name = file)
          
          
          print(head(dataVal))
          print(dim(dataVal))
          
          dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
          gc()
          
        }
          
      }
      
    }
    
  }
  gc()
  return(dataVal_all)
  
}

df_energy_sales_lsk2 <- collate_energy_sales_lsk_div_load_data(paste0(cig_folder, '/',
                                                                      energy_sales_folder, 
                                                                      '/Energy sales/Lusaka Division'))
gc()
df_energy_sales_lsk2

unique(df_energy_sales_lsk2$...2)
unique(df_energy_sales_lsk2$sheet_name)


df_energy_sales_lsk2_cleaning <- df_energy_sales_lsk2 %>% 
  dplyr::mutate(sheet_name_recoded = stringr::str_trim(sheet_name),
                sheet_name_recoded = toupper(sheet_name_recoded),
                sheet_name_recoded = ifelse(sheet_name_recoded == "JULY",
                                            "JULY11", sheet_name_recoded),
                sheet_name_recoded = ifelse(sheet_name_recoded == "AUGUST",
                                            "AUGUST11", sheet_name_recoded),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:punct:]]", " "),

                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JANU", "JAN "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "IL", " "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "09", " 2009"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "10", " 2010"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "11", " 2011"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "12", " 2012"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "13", " 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "14", " 2014"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "20 2014", " 2014"),
                
                sheet_name_recoded = ifelse(sheet_name_recoded == "MAY CORRECTED",
                                            "MAY 2020", sheet_name_recoded),
                
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                      pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                      pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded)
  )

df_energy_sales_lsk2_cleaning

sort(unique(df_energy_sales_lsk2_cleaning$sheet_name_recoded))



df_energy_sales_lsk2_cleaning_2009 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2009) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name,
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', ADVANCE = '...5', MF = '...6',
                ACTUAL_16 = '...16', ACTUAL_19 = '...19') %>% 
  dplyr::mutate(ACTUAL = ifelse(is.na(ACTUAL_16) & is.na(ACTUAL_19),
                                NA, ifelse(is.na(ACTUAL_16),
                                                 ACTUAL_19,
                                                 ACTUAL_16)),
                SUBSTATION = zoo::na.locf(SUBSTATION)) %>% 
  dplyr::filter(!is.na(SUBSTATION),
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, ADVANCE, MF,
                ACTUAL, sheet_year_recoded, sheet_name_recoded, file_name)
  
df_energy_sales_lsk2_cleaning_2009


table(df_energy_sales_lsk2_cleaning_2009$PRESENT_MWh)

df_load_2009 <- df_energy_sales_lsk2_cleaning_2009 %>% 
  dplyr::filter(SUBSTATION == 'LSK PEAK DEMAND')
df_load_2009





df_energy_sales_lsk2_cleaning_2010 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2010) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name,
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', ADVANCE = '...5', MF = '...6',
                MVA = '...16', ACTUAL = '...17') %>% 
  dplyr::mutate(SUBSTATION = zoo::na.locf(SUBSTATION)) %>% 
  dplyr::filter(!is.na(SUBSTATION),
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2010


df_load_2010 <- df_energy_sales_lsk2_cleaning_2010 %>% 
  dplyr::filter(SUBSTATION == 'LSK PEAK DEMAND')
df_load_2010





df_energy_sales_lsk2_cleaning_2011 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2011) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name,
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', ADVANCE = '...5', MF = '...6',
                MVA = '...16', ACTUAL = '...17') %>% 
  dplyr::mutate(SUBSTATION = zoo::na.locf(SUBSTATION)) %>% 
  dplyr::filter(!is.na(SUBSTATION),
    !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                       'G&T METERING DEPARTMENT', 
                       'MONTHLY LOAD FIGURES (MWh)'),
    !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2011


df_load_2011 <- df_energy_sales_lsk2_cleaning_2011 %>% 
  dplyr::filter(SUBSTATION == 'LSK PEAK DEMAND')
df_load_2011




df_energy_sales_lsk2_cleaning_2012 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2012) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', ADVANCE = '...5', MF = '...6',
                MVA = '...16', ACTUAL = '...17') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION),
    !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                       'G&T METERING DEPARTMENT', 
                       'MONTHLY LOAD FIGURES (MWh)'),
    !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2012


df_load_2012 <- df_energy_sales_lsk2_cleaning_2012 %>% 
  dplyr::filter(SUBSTATION == 'LSK PEAK DEMAND')
df_load_2012



df_energy_sales_lsk2_cleaning_2013_a <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2013,
                sheet_name_recoded != 'JAN 2013') %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2013_a



df_energy_sales_lsk2_cleaning_2013_b <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2013,
                sheet_name_recoded == 'JAN 2013') %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', ADVANCE = '...5', MF = '...6',
                MVA = '...16', ACTUAL = '...17') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2013_b



df_energy_sales_lsk2_cleaning_2014 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2014,
                # sheet_name_recoded != 'JAN 2013'
                ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2014



df_energy_sales_lsk2_cleaning_2015 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2015,
                # sheet_name_recoded != 'JAN 2013'
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2015



df_energy_sales_lsk2_cleaning_2016 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2016,
                # sheet_name_recoded != 'JAN 2013'
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2016




df_energy_sales_lsk2_cleaning_2017 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2017,
                # sheet_name_recoded != 'JAN 2013'
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2017



df_energy_sales_lsk2_cleaning_2018 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2018,
                # sheet_name_recoded != 'JAN 2013'
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2018




df_energy_sales_lsk2_cleaning_2019 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2019,
                # sheet_name_recoded != 'JAN 2013'
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2019




df_energy_sales_lsk2_cleaning_2020 <- df_energy_sales_lsk2_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2020,
                # sheet_name_recoded != 'JAN 2013'
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    -sheet_name
  ) %>% 
  dplyr::rename(SUBSTATION = '...1', UNIT = '...2', PRESENT_MWh = '...3',
                PREVIOUS_MWh = '...4', #ADVANCE = '...5', MF = '...6',
                MVA = '...5', ACTUAL = '...6') %>% 
  dplyr::mutate(SUBSTATION = c(NA, zoo::na.locf(SUBSTATION))) %>%
  dplyr::filter(!is.na(SUBSTATION), #!is.na(PRESENT_MWh), 
                !SUBSTATION %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY LOAD FIGURES (MWh)'),
                !tolower(PRESENT_MWh) %in% tolower(c('Estimated'))) %>% 
  dplyr::select(SUBSTATION, UNIT, PRESENT_MWh, PREVIOUS_MWh, #ADVANCE, MF,
                ACTUAL, MVA, sheet_year_recoded, sheet_name_recoded, file_name)

df_energy_sales_lsk2_cleaning_2020


df_energy_sales_lsk2_cleaning_all <- dplyr::bind_rows(df_energy_sales_lsk2_cleaning_2011,
                                                      df_energy_sales_lsk2_cleaning_2012,
                                                      df_energy_sales_lsk2_cleaning_2010,
                                                      df_energy_sales_lsk2_cleaning_2009,
                                                      df_energy_sales_lsk2_cleaning_2013_a,
                                                      df_energy_sales_lsk2_cleaning_2013_b,
                                                      df_energy_sales_lsk2_cleaning_2014,
                                                      df_energy_sales_lsk2_cleaning_2015,
                                                      df_energy_sales_lsk2_cleaning_2016,
                                                      df_energy_sales_lsk2_cleaning_2017,
                                                      df_energy_sales_lsk2_cleaning_2018,
                                                      df_energy_sales_lsk2_cleaning_2019,
                                                      df_energy_sales_lsk2_cleaning_2020) %>% 
  dplyr::arrange(sheet_year_recoded) %>% 
  dplyr::rename(year = sheet_year_recoded,
                sheet_name = sheet_name_recoded)

df_energy_sales_lsk2_cleaning_all



# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'lusaka_electricity_substation_statistics_loads',
#                       df_energy_sales_lsk2_cleaning_all)





df_lsk_loads_cleaned <- df_energy_sales_lsk2_cleaning_all %>% 
  dplyr::mutate(division = 'Northern')

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'lusaka_electricity_substations_loads_data',
#                       df_lsk_loads_cleaned)



######################################### ----- NORTHERN Division Data ----- #########################################


# Function for collating energy consumption data (northern division load) from the folder
collate_energy_sales_northern_div_data <- function(FolderPathInput){
  
  dataVal_all <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  for (dir in dirs_list){
    # print(dir) ## Directory name
    
    files <- list.files(path = dir,
                        pattern = '.xls')
    # print(files)
    
    
    for (file in files){
      # print(file)  ## File name
      
      file_path <- paste0(dir, "/", file)
      print(file_path)
      
      sheet_names <- readxl::excel_sheets(path = file_path)
      
      for (sheet_name in sheet_names){
        
        print(file_path)
        print(file)
        print(sheet_name)
        
        dataVal <- readxl::read_excel(file_path,
                                      sheet = sheet_name,
                                      skip = 0,
                                      col_names = FALSE) %>%
          dplyr::mutate_all(as.character) %>%
          dplyr::mutate(sheet_name = sheet_name,
                        file_name = file)
        #
        print(head(dataVal))
        print(dim(dataVal))
        # print(dataVal[4, ])
        
        dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
        gc()
        
      }
      
    }
    
  }
  gc()
  return(dataVal_all)
  
}

df_energy_sales_north <- collate_energy_sales_northern_div_data(paste0(cig_folder, '/',
                                                                      energy_sales_folder, 
                                                                      '/Energy sales/Nothern Division'))
gc()
df_energy_sales_north

unique(df_energy_sales_north$file_name)
unique(df_energy_sales_north$sheet_name)




########## Special function for luanshya and muchinga ##########

# "/home/tembo/Documents/Consulting Work/CIG/work/E014.3_IRP/2. Implementation/1. Background Information/Demand Forecast Data - Covered by NDA/Energy sales/Nothern Division"

nothern_div_path <- paste0(cig_folder, '/', energy_sales_folder, '/Energy sales/Nothern Division')

# Function for collating energy consumption data (luanshy region, northern division load) from the folder
collate_eng_sales_lm_special_north_div_data <- function(FolderPathInput, InputFileName){
  
  dataVal_all <- NULL
  
  file_path <- paste0(FolderPathInput, "/", InputFileName)
  
  sheet_names <- readxl::excel_sheets(path = file_path)
  
  for (sheet_name in sheet_names){
    
    print(file_path)
    print(file)
    print(sheet_name)
    
    dataVal <- readxl::read_excel(file_path,
                                  sheet = sheet_name,
                                  skip = 0,
                                  col_names = FALSE) %>%
      dplyr::mutate_all(as.character) %>%
      dplyr::mutate(sheet_name = sheet_name,
                    file_name = InputFileName)
    #
    print(head(dataVal))
    print(dim(dataVal))
    # print(dataVal[4, ])
    
    dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
    gc()
    
  }
  
  gc()
  return(dataVal_all)
  
}

df_energy_sales_luanshya <- collate_eng_sales_lm_special_north_div_data(nothern_div_path,
                                                                                     "LUANSHYA  REGION - JANUARY 2021 POWER PURCHASES REPORT.xlsx")
gc()
df_energy_sales_luanshya

# unique(df_energy_sales_luanshya$file_name)
# unique(df_energy_sales_luanshya$sheet_name)


df_energy_sales_muchinga <- collate_eng_sales_lm_special_north_div_data(nothern_div_path,
                                                                                     "MUCHINGA REGION - JANUARY 2021 POWER PURCHASES REPORT.xlsx")
gc()
df_energy_sales_muchinga


######################################### NORTHERN Data ############################################

df_energy_sales_northern_xls_cleaning <- df_energy_sales_north %>% 
  dplyr::filter(file_name == "NORTHERN REGIONAL POWER PURCHASES REPORT -  JANUARY 2021.xls") %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>% 
  dplyr::rename(col1 = '...1', col2 = '...2',
                col3 = '...3', col4 = '...4',
                col5 = '...5', col6 = '...6', 
                col7_1 = '...7',
                col10 = '...10',
                SomeUseless_1 = '...11', col7_2 = '...12',
                col7_3 = '...13', SomeUseless_2 = '...15') %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded), 
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2020 CORRECTED", 
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "AMENEDED", 
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "DIVISIONAL REVISED", 
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "DIVISION", 
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded))


df_energy_sales_northern_xls_cleaning

unique(df_energy_sales_northern_xls_cleaning$sheet_name_recoded)
unique(df_energy_sales_northern_xls_cleaning$sheet_year_recoded)


df_north_2020_2021 <- df_energy_sales_northern_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded %in% c(2021,
                                          # 2019,
                                          # 2018,
                                          2020)) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col1, #"...1",
                UNIT = col2, # "...2",
                PREVIOUS_MWh = col3, #"...3",
                PRESENT_MWh = col4, #"...4",
                CONSUMPTION_MWh = col5, #"...5",
                MVA = col6, #"...6",
                REMARKS = col7_1, #"...7"
                )

df_north_2020_2021



df_north_2017_2019 <- df_energy_sales_northern_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded %in% c(2018,
                                          2017,
                                          # 2016,
                                          2019)) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col1, #"...1",
                UNIT = col2, # "...2",
                PREVIOUS_MWh = col3, #"...3",
                PRESENT_MWh = col4, #"...4",
                CONSUMPTION_MWh = col5, #"...5",
                MVA = col6, #"...6",
                REMARKS = col7_1, #"...7"
  ) %>% 
  dplyr::select(-col10, -SomeUseless_1)

df_north_2017_2019




df_north_2016 <- df_energy_sales_northern_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded %in% c(2016)) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) 

df_north_2016

df_north_2016_oct_dec <- df_north_2016 %>% 
  dplyr::filter(sheet_name_recoded %in% c('OCT 2016', 'NOV 2016', 'DEC 2016')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col1,
                UNIT = col2,
                PREVIOUS_MWh = col3,
                PRESENT_MWh = col4,
                CONSUMPTION_MWh = col5,
                MVA = col6,
                REMARKS = col7_1)

df_north_2016_oct_dec



df_north_2016_jan_sept <- df_north_2016 %>% 
  dplyr::filter(!sheet_name_recoded %in% c('OCT 2016', 'NOV 2016', 'DEC 2016')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col2,
                UNIT = col3,
                PREVIOUS_MWh = col4,
                PRESENT_MWh = col5,
                CONSUMPTION_MWh = col6,
                MVA = col7_1,
                REMARKS = col7_2
  ) %>% 
  dplyr::select(-col1)

df_north_2016_jan_sept


df_north_2016_cleaned <- dplyr::bind_rows(df_north_2016_jan_sept,
                                          df_north_2016_oct_dec)

df_north_2016_cleaned




df_north_2015 <- df_energy_sales_northern_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded %in% c(2015)) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

df_north_2015




df_north_2015_jan_apr <- df_north_2015 %>% 
  dplyr::filter(sheet_name_recoded %in% c('JAN 2015', 'FEB 2015', 
                                          'MAR 2015', 'APR 2015')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col2,
                UNIT = col3,
                PREVIOUS_MWh = col4,
                PRESENT_MWh = col5,
                CONSUMPTION_MWh = col6,
                MVA = col7_1,
                REMARKS = col7_3) %>%
  dplyr::select(-col1)

df_north_2015_jan_apr



df_north_2015_may <- df_north_2015 %>% 
  dplyr::filter(sheet_name_recoded %in% c('MAY 2015')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col1,
                UNIT = col2,
                PREVIOUS_MWh = col3,
                PRESENT_MWh = col4,
                CONSUMPTION_MWh = col5,
                MVA = col6,
                REMARKS = col7_1)

df_north_2015_may


df_north_2015_jun_dec <- df_north_2015 %>% 
  dplyr::filter(!sheet_name_recoded %in% c('JAN 2015', 'FEB 2015', 
                                           'MAR 2015', 'APR 2015',
                                           'MAY 2015')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col2,
                UNIT = col3,
                PREVIOUS_MWh = col4,
                PRESENT_MWh = col5,
                CONSUMPTION_MWh = col6,
                MVA = col7_1,
                REMARKS = col7_2
  ) %>% 
  dplyr::select(-col1)

df_north_2015_jun_dec


df_north_2015_cleaned <- dplyr::bind_rows(df_north_2015_jan_apr,
                                          df_north_2015_may,
                                          df_north_2015_jun_dec)

df_north_2015_cleaned





df_north_2014 <- df_energy_sales_northern_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded %in% c(#2013,
                                          #2017,
                                          # 2016,
                                          2014)) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col2,
                UNIT = col3,
                PREVIOUS_MWh = col4, 
                PRESENT_MWh = col5,
                CONSUMPTION_MWh = col6,
                MVA = col7_1,
                REMARKS = col7_3,
  ) %>% 
  dplyr::select(-col1, -SomeUseless_2)

df_north_2014




df_north_2013 <- df_energy_sales_northern_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded %in% c(#2013,
    #2017,
    # 2016,
    2013)) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = col1,
                UNIT = col2,
                PREVIOUS_MWh = col3, 
                PRESENT_MWh = col4,
                CONSUMPTION_MWh = col5,
                MVA = col6,
                REMARKS = col7_1,
  )

df_north_2013


df_northern_cleaning <- dplyr::bind_rows(df_north_2013,
                                         df_north_2014,
                                         df_north_2015_cleaned,
                                         df_north_2016_cleaned,
                                         df_north_2017_2019,
                                         df_north_2020_2021) 


df_northern_cleaned <- df_northern_cleaning %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in% 
                                        tolower(c('0.4KV', '11kV', '66KV',
                                                  '33kV', '220kV', '132kV',
                                                  '330kV', '88kV', "33KkV")),
                                      SUBSTATION, NA),
                SUBSTATION_2 = ifelse(SUBSTATION_2 %in% c("PENSULO                                         330kV",
                                                          "PENSULO                                         66kV"),
                                      "PENSULO", SUBSTATION_2),
                volt_kV1 = ifelse(tolower(SUBSTATION) %in%
                                      tolower(c('0.4KV', '11kV', '33kV',
                                                '66kV', '220kV', '330kV',
                                                '88kV', '132kV', "33KkV")),
                                    SUBSTATION, NA),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('66/11KV')),
                                  '66/11kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('66/33kV')),
                                  '66/33kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('330KV')),
                                  '330kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('132kV')),
                                  '132kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('88kV')),
                                  '88kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('33kV')),
                                  '33kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('11kV')),
                                  '11kV', volt_kV1),
                volt_kV1 = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                      tolower('0.4kV')),
                                  '0.4kV', volt_kV1),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('330kV')),
                                  '330kV', UNIT),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66/11KV')),
                                  '66/11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66/33kV')),
                                  '66/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66/11KV')),
                                  '66/11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('132kV')),
                                  '132kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('88kV')),
                                  '88kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('33kV')),
                                  '33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('11kV')),
                                  '11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('0.4kV')),
                                  '0.4kV', volt_kV2),
                volt_kV2 = ifelse(tolower(volt_kV2) %in%
                                    tolower(c('0.4KV', '11kV', '66KV', '33kV',
                                              '220kV', '132kV', '330kV', '88kV')),
                                  volt_kV2, NA),
                voltage_kV = ifelse(is.na(volt_kV1) & is.na(volt_kV2),
                                    NA,
                                    ifelse(is.na(volt_kV1),
                                           volt_kV2,
                                           volt_kV1)),
                
                SUBSTATION_2a = zoo::na.locf(SUBSTATION_2),
                division = 'Northern'
  ) %>% 
  dplyr::filter(!tolower(SUBSTATION_2a) %in% tolower(c('SUBSTATION', 'ZESCO LIMITED',
                                      'METERING DEPARTMENT',
                                      'POWER PURCHASES',
                                      'MONTHLY LOAD FIGURES (MWh)',
                                      'SUBSTATION',
                                      'BULK METERING FOR NORTHERN REGION',
                                      'G&T METERING DEPARTMENT', 
                                      'MONTHLY READING (MEGA WATTS HOURS)',
                                      'MONTHLY LOAD FIGURES (MWh)',
                                      'METERING DEPARTMENT',
                                      'NOTE')),
                # !stringr::str_detect(SUBSTATION_2a, "PREVIOUS MONTH"),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower("Points of imports and exports")),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower("ESTIMTED READINGS")),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower("BSPs closer")),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('LUALUO SUBSTATION ARE OUT')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('Readings Estimated')),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('transmission losses')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('high energy loss')),
                
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('TRANSMISSION METERING')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('NOTE')),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('READING SEQUENCE')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('technical errors')),
                
                
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('Contributing Factor To The Regional Loss')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('CONTRIBUTING FACTORS TO THE REGIONAL LOSS')),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('TRANSMISION METERING')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('Metering unit')),
                
                
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('ORDINALLY METERS')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('NOT WORKING')),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('metering circuit')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('CHISHIMBA BSP METER')),
                
                
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('66KV METERING AT NGOLI')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('HMI SYSTEM')),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('This however this  is still a high loss')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('Comparing with the Units sent out')),
                
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('THE METER IN MBALA ON 11 KV CLOCKED')),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower('JUNE PURHCASES DON’T INCLUDE MUCHINGA REGION'))
  )

unique(df_northern_cleaned$SUBSTATION_2a)
table(df_northern_cleaned$voltage_kV)

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'northern_electricity_substations_data',
#                       df_northern_cleaned)




######################################### Luapula Data ############################################

df_energy_sales_luapula_xls_cleaning <- df_energy_sales_north %>% 
  dplyr::filter(file_name == "JANUARY 2021 POWER PURCHASES LUAPULA.xls",
                # sheet_name %in% c("December 2013", "January 2014")
                ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded), 
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2020 CORRECTED", 
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINGS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "MAY 2013 WITH RE-BILLINS", 
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "AMENEDED", 
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "DIVISIONAL REVISED", 
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                              pattern = "DIVISION", 
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded))


df_energy_sales_luapula_xls_cleaning

yr_lst <- unique(df_energy_sales_luapula_xls_cleaning$sheet_year_recoded)
yr_lst


df_NA <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(is.na(sheet_year_recoded))

df_NA


df_2021 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2021) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_MWh = "...4",
                CONSUMPTION_MWh = "...5",
                MVA = "...6",
                REMARKS = "...7") %>% 
  dplyr::mutate(year = 2021)

df_2021



df_2020 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2020) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_MWh = "...4",
                CONSUMPTION_MWh = "...5",
                MVA = "...6",
                REMARKS = "...7") %>% 
  dplyr::mutate(year = 2020)

df_2020


df_20 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 20) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_MWh = "...4",
                CONSUMPTION_MWh = "...5",
                MVA = "...6",
                REMARKS = "...7") %>% 
  dplyr::mutate(year = 2020)

df_20


df_2020_cleaned <- dplyr::bind_rows(df_2020, df_20)
df_2020_cleaned

df_2019 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 2019) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_MWh = "...4",
                CONSUMPTION_MWh = "...5",
                MVA = "...6",
                REMARKS = "...7") %>% 
  dplyr::mutate(year = 2019)

df_2019


df_19 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 19) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

table(df_19$sheet_name_recoded)



df_19_jan_feb <- df_19 %>% 
  dplyr::filter(sheet_year_recoded == 19,
                sheet_name_recoded %in% c('JAN 19', 'FEB 19')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::mutate(year = 2019)


df_19_jan_feb
# rm(df_19_jan)



df_19_mar_dec <- df_19 %>% 
  dplyr::filter(sheet_year_recoded == 19,
                sheet_name_recoded %in% c('MAR 19', 'APR 19', 'MAY 19', 'JUN 19',
                                          'JUL 19', 'AUG 19',
                                          'SEPT 19', 'OCT 19', 'NOV 19', 'DEC 19')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_MWh = "...4",
                CONSUMPTION_MWh = "...5",
                MVA = "...6",
                REMARKS = "...7") %>% 
  dplyr::mutate(year = 2019)

df_19_mar_dec

# rm(df_19_mar)

df_19_cleaned <- dplyr::bind_rows(df_19_jan_feb,
                                  df_19_mar_dec)

df_19_cleaned


df_2019_cleaned <- dplyr::bind_rows(df_19_cleaned,
                                    df_2019)

df_2019_cleaned


df_2018 <- df_energy_sales_luapula_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded == 2018) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

df_2018

unique(df_2018$sheet_name_recoded)

df_2018_jan <- df_2018 %>% 
  dplyr::filter(sheet_year_recoded == 2018,
                sheet_name_recoded %in% c('JAN 2018')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2018)

df_2018_jan




df_2018_feb <- df_2018 %>% 
  dplyr::filter(sheet_year_recoded == 2018,
                sheet_name_recoded %in% c('FEB 2018')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2018)

df_2018_feb


df_2018_cleaned <- dplyr::bind_rows(df_2018_jan, df_2018_feb)


df_18 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 18) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

df_18



df_18_sept_dec <- df_18 %>% 
  dplyr::filter(sheet_year_recoded == 18,
                sheet_name_recoded %in% c('DEC 18', 'NOV 18',
                                          'OCT 18', 'SEPT 18')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::mutate(year = 2018) %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh,
                CONSUMPTION_MWh, sheet_name, file_name, MVA, REMARKS,
                sheet_name_recoded, sheet_year_recoded, year)

df_18_sept_dec



df_18_apr_aug <- df_18 %>% 
  dplyr::filter(sheet_year_recoded == 18,
                sheet_name_recoded %in% c('AUG 18', 'JUN 18',
                                          'MAY 18', 'APR 18',
                                          'JUL 18')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::mutate(year = 2018) %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh,
                CONSUMPTION_MWh, sheet_name, file_name, MVA, REMARKS,
                sheet_name_recoded, sheet_year_recoded, year)

df_18_apr_aug
# rm(df_18_aug)


df_18_mar <- df_18 %>% 
  dplyr::filter(sheet_year_recoded == 18,
                sheet_name_recoded %in% c('MAR 18')) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::mutate(year = 2018) %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh,
                CONSUMPTION_MWh, sheet_name, file_name, MVA, REMARKS,
                sheet_name_recoded, sheet_year_recoded, year)

df_18_mar


df_18_cleaned <- dplyr::bind_rows(df_18_mar, df_18_apr_aug, df_18_sept_dec)


df_2018_18_cleaned <- dplyr::bind_rows(df_2018_cleaned, df_18_cleaned)


df_2017 <- df_energy_sales_luapula_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded == 2017) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2017)

df_2017

unique(df_2017$sheet_name_recoded)



df_2016 <- df_energy_sales_luapula_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded == 2016) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

df_2016




df_2016_jan_jul <- df_2016 %>%
  dplyr::filter(#sheet_year_recoded == 2016,
    sheet_name_recoded %in% c('JAN 2016', 'FEB 2016', 'MAR 2016',
                              'MAY 2016', 'JUN 2016', 'JUL 2016')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...4",
                UNIT = "...5",
                PREVIOUS_MWh = "...6",
                PRESENT_MWh = "...7",
                CONSUMPTION_MWh = "...25",
                MVA = "...28",
                REMARKS = "...37") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2016)

df_2016_jan_jul




df_2016_apr <- df_2016 %>%
  dplyr::filter(sheet_name_recoded %in% c('APR 2016')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...4",
                UNIT = "...5",
                PREVIOUS_MWh = "...6",
                PRESENT_MWh = "...7",
                CONSUMPTION_MWh = "...25",
                MVA = "...28",
                REMARKS = "...37") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2016)

df_2016_apr


df_2016_other <- df_2016 %>%
  dplyr::filter(!sheet_name_recoded %in% c('JAN 2016', 'FEB 2016', 'MAR 2016',
                                           'MAY 2016', 'JUN 2016', 'JUL 2016',
                                           'APR 2016')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_MWh = "...6",
                CONSUMPTION_MWh = "...7",
                MVA = "...25",
                REMARKS = "...28") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2016)

df_2016_other


df_2016_cleaned <- dplyr::bind_rows(df_2016_other,
                                    df_2016_apr,
                                    df_2016_jan_jul)

df_2016_cleaned




df_2015 <- df_energy_sales_luapula_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded == 2015) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

df_2015

unique(df_2015$sheet_name_recoded)




df_2015_jan_jul <- df_2015 %>%
  dplyr::filter(sheet_name_recoded %in% c("JUL 2015", "JUN 2015", "MAY 2015",
                                           "APR 2015", "MAR 2015", "FEB 2015", 
                                           "JAN 2015")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...6",
                UNIT = "...7",
                PREVIOUS_MWh = "...25",
                PRESENT_MWh = "...28",
                CONSUMPTION_MWh = "...37",
                MVA = "...38",
                REMARKS = "...39") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2015)

df_2015_jan_jul



df_2015_aug_dec <- df_2015 %>%
  dplyr::filter(!sheet_name_recoded %in% c("JUL 2015", "JUN 2015", "MAY 2015",
                                          "APR 2015", "MAR 2015", "FEB 2015", 
                                          "JAN 2015")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...4",
                UNIT = "...5",
                PREVIOUS_MWh = "...6",
                PRESENT_MWh = "...7",
                CONSUMPTION_MWh = "...25",
                MVA = "...28",
                REMARKS = "...37") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2015)

df_2015_aug_dec


df_2015_cleaned <- dplyr::bind_rows(df_2015_aug_dec,
                                    df_2015_jan_jul)

df_2015_cleaned


df_2014 <- df_energy_sales_luapula_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded == 2014) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  )

df_2014

unique(df_2014$sheet_name_recoded)


df_2014_oct_dec <- df_2014 %>%
  dplyr::filter(sheet_name_recoded %in% c("DEC 2014", "OCT 2014", #"SEPT 2014",
                                           # "APR 2015", "MAR 2015", "FEB 2015",
                                           "NOV 2014")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...6",
                UNIT = "...7",
                PREVIOUS_MWh = "...25",
                PRESENT_MWh = "...28",
                CONSUMPTION_MWh = "...37",
                MVA = "...38",
                REMARKS = "...39") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2014)

df_2014_oct_dec



df_2014_aug_sept <- df_2014 %>%
  dplyr::filter(sheet_name_recoded %in% c("SEPT 2014", "AUG 2014")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...25",
                UNIT = "...28",
                PREVIOUS_MWh = "...37",
                PRESENT_MWh = "...38",
                CONSUMPTION_MWh = "...39",
                MVA = "...40",
                REMARKS = "...41") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2014)

df_2014_aug_sept



df_2014_jun_jul <- df_2014 %>%
  dplyr::filter(sheet_name_recoded %in% c("JUL 2014",
                                          "JUN 2014")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...37",
                UNIT = "...38",
                PREVIOUS_MWh = "...39",
                PRESENT_MWh = "...40",
                CONSUMPTION_MWh = "...41",
                MVA = "...42",
                REMARKS = "...43") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2014)

df_2014_jun_jul


df_2014_jan_may <- df_2014 %>%
  dplyr::filter(sheet_name_recoded %in% c("MAY 2014", "APR 2014",
                                          "MAR 2014", "FEB 2014",
                                          "JAN 2014")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...39",
                UNIT = "...40",
                PREVIOUS_MWh = "...41",
                PRESENT_MWh = "...42",
                CONSUMPTION_MWh = "...43",
                MVA = "...44",
                REMARKS = "...45") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2014)

df_2014_jan_may


df_2014_cleaned <- dplyr::bind_rows(df_2014_jan_may,
                                    df_2014_jun_jul,
                                    df_2014_aug_sept,
                                    df_2014_oct_dec)

df_2014_cleaned


df_2013 <- df_energy_sales_luapula_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded == 2013) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...38",
                UNIT = "...39",
                PREVIOUS_MWh = "...40",
                PRESENT_MWh = "...41",
                CONSUMPTION_MWh = "...42",
                MVA = "...43",
                REMARKS = "...44") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2013)

df_2013

unique(df_2013$sheet_name_recoded)

df_4 <- df_energy_sales_luapula_xls_cleaning %>% 
  dplyr::filter(sheet_year_recoded == 4)

df_4


df_luapula_cleaning <- dplyr::bind_rows(df_2013,
                                        df_2014_cleaned, 
                                        df_2015_cleaned, 
                                        df_2016_cleaned,
                                        df_2017,
                                        df_2018_18_cleaned,
                                        df_2019_cleaned,
                                        df_2020_cleaned,
                                        df_2021)

df_luapula_cleaning

df_luapula_cleaned <- df_luapula_cleaning %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in% 
                                        tolower(c('0.4KV', '11kV', '66KV',
                                                  '33kV', '220kV', '132kV',
                                                  '330kV', '88kV')),
                                      SUBSTATION, NA),
                SUBSTATION_2 = ifelse(SUBSTATION_2 %in% c("PENSULO                                         330kV",
                                                          "PENSULO                                         66kV"),
                                      "PENSULO", SUBSTATION_2),
                volt_kV1 = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('0.4KV', '11kV', '33kV',
                                              '66kV', '220kV', '330kV',
                                              '88kV', '132kV')),
                                  SUBSTATION, NA),
                
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('330kV')),
                                  '330kV', UNIT),
                # volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                #                                       tolower("33\\11kV")), # this gives an error
                #                   '33/11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66/11KV')),
                                  '66/11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66/33kV')),
                                  '66/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66//11KV')),
                                  '66/11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('66/33kV')),
                                  '66/33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('88kV')),
                                  '88kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('33kV')),
                                  '33kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('11kV')),
                                  '11kV', volt_kV2),
                volt_kV2 = ifelse(stringr::str_detect(tolower(UNIT),
                                                      tolower('0.4kV')),
                                  '0.4kV', volt_kV2),
                volt_kV2 = ifelse(tolower(volt_kV2) %in%
                                    tolower(c('0.4KV', '11kV', '66KV', '33kV',
                                              '220kV', '132kV', '330kV', '88kV')),
                                  volt_kV2, NA),
                voltage_kV = ifelse(is.na(volt_kV1) & is.na(volt_kV2),
                                    NA,
                                    ifelse(is.na(volt_kV1),
                                           volt_kV2,
                                           volt_kV1)),
                
                SUBSTATION_2a = zoo::na.locf(SUBSTATION_2),
                division = 'Northern'
  ) %>% 
  dplyr::filter(!SUBSTATION_2a %in% c('SUBSTATION', 'ZESCO LIMITED',
                                   'METERING DEPARTMENT',
                                   'POWER PURCHASES',
                                   'MONTHLY LOAD FIGURES (MWh)',
                                   'SUBSTATION',
                                   'BULK METERING FOR LUAPULA REGION',
                                   'G&T METERING DEPARTMENT', 
                                   'MONTHLY READING (MEGA WATTS HOURS)',
                                   'MONTHLY LOAD FIGURES (MWh)',
                                   'METERING DEPARTMENT',
                                   'NOTE'),
                !stringr::str_detect(SUBSTATION_2a, "PREVIOUS MONTH"),
                !stringr::str_detect(SUBSTATION_2a, "Points of imports and exports"),
                !stringr::str_detect(SUBSTATION_2a, "Mbereshi BSP"),
                !stringr::str_detect(SUBSTATION_2a, "BSPs closers")
                )

df_luapula_cleaned



# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'luapula_electricity_substations_data',
#                       df_luapula_cleaned)


df_luapula_trade_summary <- df_luapula_cleaned %>% 
  dplyr::filter(tolower(SUBSTATION_2a) %in% tolower(c('TOTAL IMPORT',
                                                   'TOTAL EXPORT',
                                                   'NET PURCHASES',
                                                   'CMS SALES',
                                                   'PREPAID SALES',
                                                   'LAFARGE SALES',
                                                   'TOTAL REGIONAL SALES',
                                                   'LOSS (MWh)',
                                                   'LOSSES (%)',
                                                   'NET PURCHASE'))) %>% 
  dplyr::mutate(SUBSTATION_2a = ifelse(tolower(SUBSTATION_2a) ==
                                      tolower('LOSSES (%)'),
                                    'LOSSES', SUBSTATION_2a)) %>%
  dplyr::select(SUBSTATION_2a, CONSUMPTION_MWh,
                sheet_name_recoded, file_name) %>%
  dplyr::rename(variable = SUBSTATION_2a,
                sheet_name = sheet_name_recoded,
                consumption_kwh = CONSUMPTION_MWh)

df_luapula_trade_summary

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'luapula_electricity_trade_summary',
#                       df_luapula_trade_summary)



df_luapula_trade_statistics <- df_luapula_cleaned %>% 
  dplyr::filter(!tolower(SUBSTATION_2a) %in% tolower(c('TOTAL IMPORT',
                                                      'TOTAL EXPORT',
                                                      'NET PURCHASES',
                                                      'CMS SALES',
                                                      'PREPAID SALES',
                                                      'LAFARGE SALES',
                                                      'TOTAL REGIONAL SALES',
                                                      'LOSS (MWh)',
                                                      'LOSSES (%)',
                                                      'NET PURCHASE')))

df_luapula_trade_statistics

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'luapula_electricity_substation_statistics',
#                       df_luapula_trade_statistics)





######################################### Ndola Data ############################################

df_energy_sales_ndola_xls_cleaning <- df_energy_sales_north %>% 
  dplyr::filter(file_name == "NDOLA REGIONAL PURCHASE REPORT JANUARY 2021.xls",
                # sheet_name %in% c("December 2013", "January 2014")
  ) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>% 
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "RUARY", ""),
              sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded), 
                                                            pattern = "MARCH", "MAR "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "EMBER", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "OBER", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "UARY", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "JUNE-", "JUN"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "JUNE", "JUN"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "JULY", "JUL"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "UST", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "MAY 2020 CORRECTED", 
                                                            "MAY 2020"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "MAY 2013 WITH RE-BILLINGSS", 
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "MAY 2013 WITH RE-BILLINGS", 
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "MAY 2013 WITH RE-BILLINS", 
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "\\(2\\)", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "[[:punct:]]", " "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "IL", " "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "APRI 2020", "APR 2020"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "RI", "R "),
              
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "AMENEDED", 
                                                            ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "DIVISIONAL REVISED", 
                                                            ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded, 
                                                            pattern = "DIVISION", 
                                                            ""),
              sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
              sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
              
              sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "[[:alpha:]]", ""),
              sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                            pattern = "[[:space:]]", ""),
              sheet_year_recoded = as.numeric(sheet_year_recoded)) %>% 
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_MWh = "...4",
                CONSUMPTION_MWh = "...5",
                MVA = "...6",
                MW = "...7",
                REMARKS = "...16")


df_energy_sales_ndola_xls_cleaning

unique(df_energy_sales_ndola_xls_cleaning$MW)




df_ndola_cleaned <- df_energy_sales_ndola_xls_cleaning %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in% 
                                        tolower(c('0.4KV', '11kV', '66KV',
                                                  '33kV', '220kV', '132kV',
                                                  '330kV', '88kV', "33KkV")),
                                      SUBSTATION, NA),
                voltage_kV = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('0.4KV', '11kV', '33kV',
                                              '66kV', '220kV', '330kV',
                                              '88kV', '132kV', "33KkV")),
                                  SUBSTATION, NA),
                SUBSTATION_2a = zoo::na.locf(SUBSTATION_2),
                division = 'Northern'
  ) %>% 
  dplyr::filter(!SUBSTATION_2a %in% c('SUBSTATION', 'ZESCO LIMITED',
                                      'METERING DEPARTMENT',
                                      'POWER PURCHASES',
                                      'MONTHLY LOAD FIGURES (MWh)',
                                      'SUBSTATION',
                                      'BULK METERING FOR NDOLA REGION',
                                      'G&T METERING DEPARTMENT', 
                                      'MONTHLY READING (MEGA WATTS HOURS)',
                                      'MONTHLY LOAD FIGURES (MWh)',
                                      'METERING DEPARTMENT',
                                      'NOTE'),
                # !stringr::str_detect(SUBSTATION_2a, "PREVIOUS MONTH"),
                !stringr::str_detect(SUBSTATION_2a, "Points of imports and exports"),
                !stringr::str_detect(SUBSTATION_2a, "Mbereshi BSP"),
                !stringr::str_detect(SUBSTATION_2a, "BSPs closers")
  )

df_ndola_cleaned


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'ndola_electricity_substations_data',
#                       df_ndola_cleaned)



df_ndola_trade_summary <- df_ndola_cleaned %>% 
  dplyr::filter(tolower(SUBSTATION_2a) %in% tolower(c('TOTAL IMPORT',
                                                      'TOTAL EXPORT',
                                                      'NET PURCHASES',
                                                      'CMS SALES',
                                                      'PREPAID SALES',
                                                      'LAFARGE SALES',
                                                      'TOTAL REGIONAL SALES',
                                                      'LOSS (MWh)',
                                                      'LOSSES (%)',
                                                      'NET PURCHASE',
                                                      "3E-SALES",                                                                                                                                               
                                                      "E-CASH SALES",
                                                      "TOTAL CMS SALES")) |
                stringr::str_detect(SUBSTATION_2a, "PREVIOUS MONTH")) %>% 
  dplyr::mutate(SUBSTATION_2a = ifelse(tolower(SUBSTATION_2a) ==
                                         tolower('LOSSES (%)'),
                                       'LOSSES', SUBSTATION_2a)) %>%
  dplyr::select(SUBSTATION_2a, CONSUMPTION_MWh,
                sheet_name_recoded, file_name) %>%
  dplyr::rename(variable = SUBSTATION_2a,
                sheet_name = sheet_name_recoded,
                consumption_kwh = CONSUMPTION_MWh)

df_ndola_trade_summary

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'ndola_electricity_trade_summary',
#                       df_ndola_trade_summary)



df_ndola_trade_statistics <- df_ndola_cleaned %>% 
  dplyr::filter(!tolower(SUBSTATION_2a) %in% tolower(c('TOTAL IMPORT',
                                                       'TOTAL EXPORT',
                                                       'NET PURCHASES',
                                                       'CMS SALES',
                                                       'PREPAID SALES',
                                                       'LAFARGE SALES',
                                                       'TOTAL REGIONAL SALES',
                                                       'LOSS (MWh)',
                                                       'LOSSES (%)',
                                                       'NET PURCHASE',
                                                       "3E-SALES",                                                                                                                                               
                                                       "E-CASH SALES",
                                                       "TOTAL CMS SALES")),
                !stringr::str_detect(tolower(SUBSTATION_2a), tolower("PREVIOUS MONTH")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("Prepayment meter replacements")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("Credit new meter")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("Credit  meter")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("Credit Meter")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("NOTE")),
                
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("New installation")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("FOR POWER FACTOR")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("AND SHOULD BE ADDED")),
                
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("New installation")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("METERS INSTALLED")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("TO BE UPDATED")),
                
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("BWANA MKUBWA AND THE SWITCHING WAS REVERSED ON 31.08.2018 HENCE THE NIGH DEMAND AND CONSUMPTION RECORDED.")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("CLOSING READINGS WERE, 393844.3MWH AND 547366.1MWH RESPECTIVELY.")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("CREDIT - 3 COMMERCIALS")),
                !stringr::str_detect(tolower(SUBSTATION_2a),
                                     tolower("PREPAYMENT  -  76 SINGLE PHASE METERS AND 02 THREE PHASE METERS.")))

df_ndola_trade_statistics

unique(df_ndola_trade_statistics$SUBSTATION_2a)
table(df_ndola_trade_statistics$SUBSTATION_2a)

rm(df_ndola_dec)

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'ndola_electricity_substation_statistics',
#                       df_ndola_trade_statistics)






yr_lst <- unique(df_energy_sales_ndola_xls_cleaning$sheet_year_recoded)
yr_lst



df_ndola_dec <- df_energy_sales_ndola_xls_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2013, 2014),
                # sheet_name_recoded %in% c('JAN 2015', 'FEB 2015', 'MAR 2015',
                #                           'APR 2015', 'MAY 2015')
                ) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...38",
                UNIT = "...39",
                PREVIOUS_MWh = "...40",
                PRESENT_MWh = "...41",
                CONSUMPTION_MWh = "...42",
                MVA = "...43",
                REMARKS = "...44") %>% 
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_MWh, CONSUMPTION_MWh,
                sheet_name, file_name, MVA, REMARKS, 
                sheet_name_recoded, sheet_year_recoded) %>% 
  dplyr::mutate(year = 2013)

df_ndola_dec


unique(df_energy_sales_ndola_xls_cleaning$...18) == unique(df_ndola_dec$...18)





######################################### Muchinga Data ############################################

df_energy_sales_muchinga_xlsx_cleaning <- df_energy_sales_muchinga %>%
# df_energy_sales_muchinga_xlsx_cleaning <- df_energy_sales_north %>%
  dplyr::filter(file_name == "MUCHINGA REGION - JANUARY 2021 POWER PURCHASES REPORT.xlsx") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2020 CORRECTED",
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RI", "R "),

                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "AMENEDED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISIONAL REVISED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISION",
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded))


head(df_energy_sales_muchinga_xlsx_cleaning)




df_muchinga_2021 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2021)) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...5",
                MAIN_MVA = "...6",
                CHECK_MVA = "...7",
                VARIANCE_SHARE = "...8") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2021

unit_lst_2021 <- unique(df_muchinga_2021$UNIT)
unit_lst_2021




df_muchinga_2020_p1 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2020),
                !sheet_name_recoded %in% c('FEB 2020', 'JAN 2020')) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...5",
                MAIN_MVA = "...6",
                CHECK_MVA = "...7",
                VARIANCE_SHARE = "...8") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2020_p1

unit_lst_2020_p1 <- unique(df_muchinga_2020_p1$UNIT)
unit_lst_2020_p1




df_muchinga_2020_p2 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2020),
                sheet_name_recoded %in% c('FEB 2020', 'JAN 2020')) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...6",
                MAIN_MVA = "...7",
                CHECK_MVA = "...8",
                VARIANCE_SHARE = "...11") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2020_p2

unit_lst_2020_p2 <- unique(df_muchinga_2020_p2$UNIT)
unit_lst_2020_p2


#c('JUL 2019', 'JAN 2019')

df_muchinga_2019_p1 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2019),
                !sheet_name_recoded %in% c('JUL 2019', 'JAN 2019')
                ) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...6",
                MAIN_MVA = "...7",
                CHECK_MVA = "...8",
                VARIANCE_SHARE = "...11") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2019_p1

unit_lst_2019_p1 <- unique(df_muchinga_2019_p1$UNIT)
unit_lst_2019_p1




#c('JUL 2019', 'JAN 2019')

df_muchinga_2019_p2 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2019),
                sheet_name_recoded %in% c('JUL 2019')
  ) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...6",
                MAIN_MVA = "...7",
                CHECK_MVA = "...8",
                VARIANCE_SHARE = "...11") %>% 
  dplyr::select(-"...14") %>%
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2019_p2

unit_lst_2019_p2 <- unique(df_muchinga_2019_p2$UNIT)
unit_lst_2019_p2




#c('JUL 2019', 'JAN 2019')

df_muchinga_2019_p3 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2019),
                sheet_name_recoded %in% c('JAN 2019')
  ) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...2",
                UNIT = "...3",
                ACTUAL_PURCHASE_MWh = "...4",
                MAIN_MWh = "...5",
                CHECK_MWh = "...7",
                MAIN_MVA = "...8",
                CHECK_MVA = "...11",
                VARIANCE_SHARE = "...14") %>% 
  dplyr::select(-"...6",
                -"...16") %>%
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2019_p3

unit_lst_2019_p3 <- unique(df_muchinga_2019_p3$UNIT)
unit_lst_2019_p3




df_muchinga_2018 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2018),
                # sheet_name_recoded %in% c('JAN 2019')
  ) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...6",
                MAIN_MVA = "...7",
                CHECK_MVA = "...8",
                VARIANCE_SHARE = "...11") %>% 
  dplyr::select(-"...5",
                # -"...16"
                ) %>%
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2018

unit_lst_2018 <- unique(df_muchinga_2018$UNIT)
unit_lst_2018



df_muchinga_2017 <- df_energy_sales_muchinga_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(17),
                # sheet_name_recoded %in% c('JAN 2019')
  ) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                ACTUAL_PURCHASE_MWh = "...3",
                MAIN_MWh = "...4",
                CHECK_MWh = "...6",
                MAIN_MVA = "...7",
                CHECK_MVA = "...8",
                VARIANCE_SHARE = "...11") %>% 
  # dplyr::select(-"...5",
  # -"...16"
  # ) %>%
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1kV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1kV', '1kV')),
                                      SUBSTATION, NA),
                readings = ifelse(readings == "Advance (MWh)",
                                  "Advance", readings),
                
                readings = ifelse(readings == "Advance",
                                  "Advance_MWh", readings),
                readings = ifelse(readings == "Present Reading",
                                  "Present_MWh", readings),
                readings = ifelse(readings == "Previous Reading",
                                  "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_muchinga_2017

unit_lst_2017 <- unique(df_muchinga_2017$UNIT)
unit_lst_2017

muchinga_unit_lst <- unique(c(unit_lst_2017, unit_lst_2018, 
                              unit_lst_2019_p1, unit_lst_2019_p2,
                              unit_lst_2019_p3, unit_lst_2020_p1,
                              unit_lst_2020_p2, unit_lst_2021))

muchinga_unit_lst
length(muchinga_unit_lst)

muchinga_unit_lst2 <- c("DEC 17", "UNIT", "MPIKA", "MUNUNGA QUARY",
                        "TX1 IMPORT", "TX2 IMPORT", "CHINSALI LINE",
                        "CHALABESA LINE", "CHAMBESHI IMPORT", "TX1 CHINSALI BOMA",
                        "CHOZI LINE", "TANZANIA EXPORT(MBOZI)", "WULONGO TX1",
                        "WULONGO TX2", "SHIWANG'ANDU LINE IMPORT", "MATUMBO",
                        "NOV 17", "DECEMBER '18", "MUNUNGA QUARRY", 
                        "TX1 CHINSALI BOMA 66/11kV Tx", "TX 2 CHINSALI 66/33kv TX",
                        "NOVEMBER '18", "OCTOBER '18", "SEPTEMBER '18",
                        "AUGUST '18", "JULY '18", "JUNE '18", "MAY '18",
                        "APR '18", "MAR '18", "FEB '18", "MPIKA 1 & 2",
                        "SHIWANG'ANDU LINE EXPORT", "JANUARY '19",
                        "MUNUNGA QUARRY(11kV)", "TX1 IMPORT(11kV)",
                        "TX2 IMPORT(11kV)", "CHALABESA LINE(11kV)",
                        "CHAMBESHI IMPORT(33kV)", "TANZANIA EXPORT(MBOZI)(33kV)",
                        "SHIWANG'ANDU LINE EXPORT(33kV)", "MATUMBO(11Kv)",
                        "TX1 IMPORT(33kV)")

length(muchinga_unit_lst2)


df_muchinga_cleaning <- dplyr::bind_rows(df_muchinga_2017,
                                         df_muchinga_2018,
                                         df_muchinga_2019_p1,
                                         df_muchinga_2019_p2,
                                         df_muchinga_2019_p3,
                                         df_muchinga_2020_p1,
                                         df_muchinga_2020_p2,
                                         df_muchinga_2021)

df_muchinga_cleaning

df_xxx <- df_muchinga_cleaning %>% 
  dplyr::select(SUBSTATION) %>%

  # tolower() %>% 
  dplyr::filter(!is.na(SUBSTATION),
                !stringr::str_detect(tolower(SUBSTATION), "reading"),
                !stringr::str_detect(tolower(SUBSTATION), "advance"),
                stringr::str_detect(tolower(SUBSTATION), "kv"),
                !stringr::str_detect(tolower(SUBSTATION), "region is now being supplied at"),
                !stringr::str_detect(tolower(SUBSTATION), "highlighted in red")) %>% 
  dplyr::arrange(SUBSTATION) %>% 
  unique()
  

df_xxx

df_muchinga_cleaned <- df_muchinga_cleaning %>% 
  dplyr::mutate(test_kv  = ifelse(stringr::str_detect(tolower(SUBSTATION), "kv"),
                                  TRUE, FALSE),
                test_other  = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                        "region is now being supplied at"),
                                  FALSE, TRUE),

                test_other2  = ifelse(stringr::str_detect(tolower(SUBSTATION),
                                                        "highlighted in red"),
                                  FALSE, TRUE),
                
                test_true = test_kv & test_other & test_other2,
                
                voltage_kV_2 = ifelse(test_true,
                                      stringr::str_replace(SUBSTATION,
                                                           "KASAMA",
                                                           ""),
                                      NA),
                
                ## Start from here ------------------------------------------------------
                SUBSTATION_2c = stringr::str_replace(SUBSTATION_2b,
                                                     "66kV", ""),
                
                voltage_kV_2 = stringr::str_trim(voltage_kV_2),
                SUBSTATION_2c = stringr::str_trim(SUBSTATION_2c),
                
                len_sheet_name = length(sheet_name),
                len_volt_kV2 = length(zoo::na.locf(voltage_kV_2)),
                len_NA2 = len_sheet_name - len_volt_kV2,
                voltage_kV_3 = c(rep(NA, times = mean(len_NA2)),
                               zoo::na.locf(voltage_kV_2)),
                
                len_subst_2c = length(zoo::na.locf(SUBSTATION_2c)),
                len_NA_2c = len_sheet_name - len_subst_2c,
                SUBSTATION_2d = c(rep(NA, times = mean(len_NA_2c)),
                                 zoo::na.locf(SUBSTATION_2c)),
                
                UNIT_3 = ifelse(UNIT %in% muchinga_unit_lst2,
                                UNIT, NA),
                
                len_UNIT_2 = length(zoo::na.locf(UNIT_3)),
                len_NA_3 = len_sheet_name - len_UNIT_2,
                UNIT_4 = c(rep(NA, times = mean(len_NA_3)),
                                 zoo::na.locf(UNIT_3)),
                
                SUBSTATION_2e = stringr::str_replace(SUBSTATION_2d,
                                                     "1kV", "")
                
                
                ) %>% 
  dplyr::select(-len_volt_kV2, -len_NA2, -test_kv, -test_other,
                -test_other2, -test_true, -len_sheet_name,
                -len_UNIT_2, -len_NA_3)

df_muchinga_cleaned

unique(df_muchinga_cleaned$SUBSTATION_2e)

df_muchinga_cleaned2  <- df_muchinga_cleaned %>%
  dplyr::rename_with(tolower) %>% 
  dplyr::mutate(region = 'muchinga',
                # # division = 'northern'
  )

df_muchinga_cleaned2

# # RSQLite::dbWriteTable(dbConnect(SQLite(),
# #                                 paste0(write_to_db_path,
# #                                        "/",
# #                                        datalake_sqlite)),
# #                       name =  'muchinga_electricity_substations_data_new',
# #                       df_muchinga_cleaned)
# # 
# 

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'muchinga_electricity_substations_data_new_24april',
#                       df_muchinga_cleaned2)


# colnames(df_muchinga_cleaned2)
# 
# 
# df_muchinga_cleaned3 <- df_muchinga_cleaned2 %>%
#   dplyr::rename_with(tolower) %>%
#   dplyr::select(substation = substation_2d, #substation,
#                 unit = unit_4, voltage_kv = voltage_kv_3, #actual_purchase_mwh,
#                 main_mwh, check_mwh, main_mva, check_mva,
#                 variance_share, readings, sheet_year_recoded, file_name,
#                 sheet_name,  sheet_name_recoded, region, division) %>% #"actual_purchase_mwh",
#   dplyr::filter(!is.na(readings)) %>%
#   dplyr::mutate(reading_names = rep(c('pres_mwh', 'prev_mwh', 'adv_mwh'), 
#                                     length(sheet_name)/
#                                       length(c('pres_mwh', 'prev_mwh', 'adv_mwh'))),
#                 main_readings = paste0('main_', reading_names),
#                 check_readings = paste0('check_', reading_names),
#   ) %>%
#   dplyr::select(-readings) %>%
#   tidyr::pivot_wider(names_from = main_readings,
#                      values_from = main_mwh) %>%
#   tidyr::pivot_wider(names_from = check_readings,
#                      values_from = check_mwh)
# 
# df_muchinga_cleaned3
# colnames(df_muchinga_cleaned3)
# 
# 
# much_prim_var_lst <- c('substation', 'unit', 'voltage_kv', 
#                         'sheet_year_recoded', 'file_name', 'sheet_name',
#                         'sheet_name_recoded', 'division', 'region')
# 
# df_much_prim_key <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst)) %>% 
#   # dplyr::select(substation_2c, unit_4, voltage_kv_3, sheet_year_recoded, file_name, sheet_name, sheet_name_recoded, division, region) %>% 
#   dplyr::distinct()
# 
# df_much_prim_key
# 
# 
# df_much_main_mva <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), main_mva) %>% 
#   dplyr::filter(!is.na(main_mva))
# 
# df_much_main_mva
# 
# 
# df_much_check_mva <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), check_mva) %>% 
#   dplyr::filter(!is.na(check_mva))
# 
# df_much_check_mva
# 
# 
# df_much_Main_Pres_MWh <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), main_pres_mwh) %>% 
#   dplyr::filter(!is.na(main_pres_mwh))
# 
# df_much_Main_Pres_MWh
# 
# 
# df_much_Main_Prev_MWh <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), main_prev_mwh) %>% 
#   dplyr::filter(!is.na(main_prev_mwh))
# 
# df_much_Main_Prev_MWh
# 
# df_much_Main_Adv_MWh <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), main_adv_mwh) %>% 
#   dplyr::filter(!is.na(main_adv_mwh))
# 
# df_much_Main_Adv_MWh
# 
# 
# 
# df_much_Check_Pres_MWh <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), check_pres_mwh) %>% 
#   dplyr::filter(!is.na(check_pres_mwh))
# 
# df_much_Check_Pres_MWh
# 
# 
# df_much_Check_Prev_MWh <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), check_prev_mwh) %>% 
#   dplyr::filter(!is.na(check_prev_mwh))
# 
# df_much_Check_Prev_MWh
# 
# df_much_Check_Adv_MWh <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), check_adv_mwh) %>% 
#   dplyr::filter(!is.na(check_adv_mwh))
# 
# df_much_Check_Adv_MWh
# 
# 
# ##
# df_much_main_mva <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), main_mva) %>% 
#   dplyr::filter(!is.na(main_mva))
# 
# df_much_main_mva
# 
# df_much_check_mva <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), check_mva) %>% 
#   dplyr::filter(!is.na(check_mva))
# 
# df_much_check_mva
# 
# df_much_variance_share <- df_muchinga_cleaned3 %>% 
#   dplyr::select(all_of(much_prim_var_lst), variance_share) %>% 
#   dplyr::filter(!is.na(variance_share))
# 
# df_much_variance_share
# 
# 
# 
# 
# df_muchinga_cleaned4 <- dplyr::full_join(df_much_prim_key,
#                                          df_much_Main_Pres_MWh,
#                                          by = much_prim_var_lst) %>% 
#   dplyr::full_join(df_much_Check_Pres_MWh,
#                    by = much_prim_var_lst) %>% 
#   dplyr::full_join(df_much_Main_Prev_MWh,
#                    by = much_prim_var_lst) %>% 
#   dplyr::full_join(df_much_Check_Prev_MWh,
#                    by = much_prim_var_lst) %>% 
#   dplyr::full_join(df_much_Main_Adv_MWh,
#                    by = much_prim_var_lst) %>% 
#   dplyr::full_join(df_much_Check_Adv_MWh,
#                    by = much_prim_var_lst) %>%
#   dplyr::full_join(df_much_main_mva,
#                    by = much_prim_var_lst) %>% 
#   
#   ### These two dataframes below were not included ---------------------
#   # dplyr::full_join(df_much_check_mva,
#   #                  by = much_prim_var_lst) %>% 
#   # dplyr::full_join(df_much_variance_share,
#   #                  by = much_prim_var_lst) %>%
#   
#   dplyr::rename(main_consump_mwh = main_adv_mwh,
#                 check_consump_mwh = check_adv_mwh,
#                 # voltage = voltage_kv_3,
#                 year = sheet_year_recoded,
#                 # substation = substation_2c,
#                 # unit = unit_4 
#   )
# 
# df_muchinga_cleaned4
# 
# table(df_muchinga_cleaned4$year)



######################################### Luanshya Data ############################################

# df_energy_sales_luanshya_xlsx_cleaning <- df_energy_sales_north %>%
df_energy_sales_luanshya_xlsx_cleaning <- df_energy_sales_luanshya %>%
  dplyr::filter(file_name == "LUANSHYA  REGION - JANUARY 2021 POWER PURCHASES REPORT.xlsx") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "RUARY", ""),
              sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                            pattern = "MARCH", "MAR "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "EMBER", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "OBER", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "UARY", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "JUNE-", "JUN"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "JUNE", "JUN"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "JULY", "JUL"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "UST", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2020 CORRECTED",
                                                            "MAY 2020"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2013 WITH RE-BILLINGS",
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2013 WITH RE-BILLINS",
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "\\(2\\)", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "[[:punct:]]", " "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "IL", " "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "APRI 2020", "APR 2020"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "RI", "R "),
              
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "AMENEDED",
                                                            ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "DIVISIONAL REVISED",
                                                            ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "DIVISION",
                                                            ""),
              sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
              sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
              
              sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "[[:alpha:]]", ""),
              sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                            pattern = "[[:space:]]", ""),
              sheet_year_recoded = as.numeric(sheet_year_recoded)) #%>%
  # dplyr::select(-'...25')


head(df_energy_sales_luanshya_xlsx_cleaning)

unique(df_energy_sales_luanshya_xlsx_cleaning$sheet_year_recoded)


df_luanshya_p1 <- df_energy_sales_luanshya_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2021, 2020, 2019, 2018, 2017, 18, 17),
                !sheet_name_recoded %in% c("DEC 18", "NOV 18", "OCT 18", 
                                           "SEPT 18", "JAN 2019")) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...2",
                UNIT = "...3",
                ACTUAL_PURCHASE_MWh = "...4",
                MAIN_MWh = "...5",
                CHECK_MWh = "...7",
                MAIN_MVA = "...8",
                CHECK_MVA = "...9",
                VARIANCE_SHARE = "...10") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV','3.3kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1KV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV','3.3kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1KV', '1kV')),
                                      SUBSTATION, NA),
                # readings = ifelse(readings == "Advance (MWh)",
                #                   "Advance", readings),
                # 
                # readings = ifelse(readings == "Advance",
                #                   "Advance_MWh", readings),
                # readings = ifelse(readings == "Present Reading",
                #                   "Present_MWh", readings),
                # readings = ifelse(readings == "Previous Reading",
                #                   "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_luanshya_p1
unique(df_luanshya_p1$...1)
unique(df_luanshya_p1$...13)
unique(df_luanshya_p1$...14)
unique(df_luanshya_p1$...15)
       

table(df_luanshya$...13)
table(df_luanshya$...14)
table(df_luanshya$...15)



# df_energy_sales_luanshya_xlsx_cleaning <- df_energy_sales_north %>%
df_energy_sales_luanshya_xlsx_cleaning <- df_energy_sales_luanshya %>%
  dplyr::filter(file_name == "LUANSHYA  REGION - JANUARY 2021 POWER PURCHASES REPORT.xlsx") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "RUARY", ""),
              sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                            pattern = "MARCH", "MAR "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "EMBER", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "OBER", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "UARY", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "JUNE-", "JUN"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "JUNE", "JUN"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "JULY", "JUL"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "UST", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2020 CORRECTED",
                                                            "MAY 2020"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2013 WITH RE-BILLINGS",
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "MAY 2013 WITH RE-BILLINS",
                                                            "MAY 2013"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "\\(2\\)", ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "[[:punct:]]", " "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "IL", " "),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "APRI 2020", "APR 2020"),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "RI", "R "),
              
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "AMENEDED",
                                                            ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "DIVISIONAL REVISED",
                                                            ""),
              sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "DIVISION",
                                                            ""),
              sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
              sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
              
              sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                            pattern = "[[:alpha:]]", ""),
              sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                            pattern = "[[:space:]]", ""),
              sheet_year_recoded = as.numeric(sheet_year_recoded)) #%>%
  # dplyr::select(-'...25')


head(df_energy_sales_luanshya_xlsx_cleaning)

unique(df_energy_sales_luanshya_xlsx_cleaning$sheet_year_recoded)


df_luanshya_p1 <- df_energy_sales_luanshya_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2021, 2020, 2019, 2018, 2017, 18, 17),
                !sheet_name_recoded %in% c("DEC 18", "NOV 18", "OCT 18", 
                                           "SEPT 18", "JAN 2019")) %>%
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...2",
                UNIT = "...3",
                ACTUAL_PURCHASE_MWh = "...4",
                MAIN_MWh = "...5",
                CHECK_MWh = "...7",
                MAIN_MVA = "...8",
                CHECK_MVA = "...9",
                VARIANCE_SHARE = "...10") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV','3.3kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1KV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV','3.3kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1KV', '1kV')),
                                      SUBSTATION, NA),
                # readings = ifelse(readings == "Advance (MWh)",
                #                   "Advance", readings),
                # 
                # readings = ifelse(readings == "Advance",
                #                   "Advance_MWh", readings),
                # readings = ifelse(readings == "Present Reading",
                #                   "Present_MWh", readings),
                # readings = ifelse(readings == "Previous Reading",
                #                   "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_luanshya_p1
unique(df_luanshya_p1$...1)
unique(df_luanshya_p1$...13)
unique(df_luanshya_p1$...14)
unique(df_luanshya_p1$...15)
 


df_luanshya_p2 <- df_energy_sales_luanshya_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c(2021, 2020, 2019, 2018, 2017, 18, 17),
                sheet_name_recoded %in% c("JAN 2019", "DEC 18", "NOV 18",
                                          "OCT 18", "SEPT 18")) %>% #, 
  # dplyr::filter(sheet_name_recoded %in% c('JUL 2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...2",
                UNIT = "...3",
                ACTUAL_PURCHASE_MWh = "...4",
                MAIN_MWh = "...5",
                CHECK_MWh = "...7",
                MAIN_MVA = "...8",
                CHECK_MVA = "...9",
                VARIANCE_SHARE = "...10") %>% 
  # dplyr::select(-"...23") %>% 
  dplyr::mutate(SUBSTATION_2 = ifelse(!tolower(SUBSTATION) %in%
                                        tolower(c('Present Reading',
                                                  'Previous Reading',
                                                  'Advance',
                                                  'Advance (MWh)')),
                                      SUBSTATION, NA),
                
                readings = ifelse(tolower(SUBSTATION) %in%
                                    tolower(c('Present Reading',
                                              'Previous Reading',
                                              'Advance',
                                              'Advance (MWh)')),
                                  SUBSTATION, NA),
                
                SUBSTATION_2a = ifelse(!tolower(SUBSTATION_2) %in%
                                         tolower(c('0.4KV', '11kV', '33kV','3.3kV',
                                                   '66kV', '220kV', '330kV',
                                                   '88kV', '132kV', '1KV', '1kV')),
                                       SUBSTATION_2, NA),
                
                voltage_kV_1 = ifelse(tolower(SUBSTATION) %in%
                                        tolower(c('0.4KV', '11kV', '33kV','3.3kV',
                                                  '66kV', '220kV', '330kV',
                                                  '88kV', '132kV', '1KV', '1kV')),
                                      SUBSTATION, NA),
                # readings = ifelse(readings == "Advance (MWh)",
                #                   "Advance", readings),
                # 
                # readings = ifelse(readings == "Advance",
                #                   "Advance_MWh", readings),
                # readings = ifelse(readings == "Present Reading",
                #                   "Present_MWh", readings),
                # readings = ifelse(readings == "Previous Reading",
                #                   "Previous_MWh", readings),
                
                # SUBSTATION_2b = zoo::na.locf(SUBSTATION_2a),
                len_sheet_name = length(sheet_name),
                
                len_substr = length(zoo::na.locf(SUBSTATION_2a)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2b = ifelse(len_NA_subst > len_NA_subst,
                                       c(rep(NA, times = mean(len_NA_subst)),
                                         zoo::na.locf(SUBSTATION_2a)),
                                       zoo::na.locf(SUBSTATION_2a)),
                
                
                len_volt_kV = length(zoo::na.locf(voltage_kV_1)),
                len_NA = len_sheet_name - len_volt_kV,
                voltage_kV = c(rep(NA, times = mean(len_NA)),
                               zoo::na.locf(voltage_kV_1)),
                len_unit = length(zoo::na.locf(UNIT)),
                len_NA_UNIT = len_sheet_name - len_unit,
                UNIT_2 = c(rep(NA, times = mean(len_NA_UNIT)),
                           zoo::na.locf(UNIT)),
                division = 'Northern'
  ) %>%
  dplyr::select(-len_sheet_name, -len_volt_kV, -len_NA,
                -len_unit, -len_NA_UNIT, #-'...23',
                -len_substr, -len_NA_subst)

df_luanshya_p2
unique(df_luanshya_p2$...1)
unique(df_luanshya_p2$...13)
unique(df_luanshya_p2$...14)
unique(df_luanshya_p2$...15)



df_luanshya <- dplyr::bind_rows(df_luanshya_p1 %>% 
                                  dplyr::select(-"...1"),
                                df_luanshya_p2 %>% 
                                  dplyr::select(-"...1", -"...13",
                                                -"...14", -"...15"))

df_luanshya





unique(df_luanshya$SUBSTATION_2b)

unit_lst_luanshya <- unique(df_luanshya$UNIT)
unit_lst_luanshya


luanshya_unit_lst <-c("JANUARY,2021", "UNIT", "TRANSF. 2AB",
                      "TRANSF 3AB", "TRANSF 4AB", "MAPOSA LUANSHYA FEEDER" ,
                      # "CEC MAIN BOARD" ,
                      "MAPOSA KITWE FEEDER",
                      # "ZESCO OHL-BOUNDARY",
                      "OUTDOOR FDR1", "OUTDOOR FDR2", "MAIN GATE SEWERAGE FDR",
                      "TOWN SITE FDR C", "TOWN SITE FDR B", "ROAN TOWNSHIP",
                      "MIKOMFWA FDR INTERLINK", "ROAN ANTELOP HOSPIT FDR",
                      "TUNNEL EXIST AHC/CEC MET", "MPATAMATO T/SHIP FDR1",
                      "MPATAMATO T/SHIP FDR2", "MPONGWE EAST 88/11KV",
                      "MPONGWE EAST 88/33KV",  "CHAMBATATA WEST 88/11KV",
                      "MUNKUMPU 88/11KV", "DECEMBER,2020", "NOVEMBER,2020",
                      "OCTOBER,2020", "SEPTEMBER,2020", "AUGUST,2020", 
                      "JUNE, 2020", #"ZESCO OHL",
                      "MAY,2020", "APRIL,2020", "MARCH,2020", "JULY, 2020",
                      "FEBRUARY,2020", "JANUARY,2020", "DECEMBER,2019",
                      "NOVEMBER,2019", "OCTOBER,2019", "SEPTEMBER,2019",
                      "AUGUST,2019", "JULY,2019", "JUNE,2019", "MAY,2019",
                      "APRIL,2019", "MARCH,2019", "FEBRUARY,2019", "JANUARY,2019")

# df_xxx <- df_luanshya %>% 
#     dplyr::select(SUBSTATION) %>%
#     
#     # tolower() %>% 
#     dplyr::filter(!is.na(SUBSTATION),
#                   !stringr::str_detect(tolower(SUBSTATION), "reading"),
#                   !stringr::str_detect(tolower(SUBSTATION), "advance"),
#                   stringr::str_detect(tolower(SUBSTATION), "kv"),
#                   # !stringr::str_detect(tolower(SUBSTATION), "region is now being supplied at"),
#                   # !stringr::str_detect(tolower(SUBSTATION), "highlighted in red")
#                   ) %>% 
#     dplyr::arrange(SUBSTATION) %>% 
#     unique()
#   
#   
#   df_xxx
  
df_luanshya_cleaned <- df_luanshya %>%
  dplyr::mutate(test_kv  = ifelse(stringr::str_detect(tolower(SUBSTATION), "kv"),
                                  TRUE, FALSE),
                test_true = test_kv, # & test_other & test_other2,
                voltage_kV_2 = ifelse(test_true,
                                      stringr::str_replace(SUBSTATION,
                                                           "LUANSHYA MUNIC",
                                                           ""),
                                      NA),
                voltage_kV_2 = stringr::str_trim(voltage_kV_2),
                len_sheet_name = length(sheet_name),
                len_volt_kV2 = length(zoo::na.locf(voltage_kV_2)),
                len_NA2 = len_sheet_name - len_volt_kV2,
                voltage_kV_3 = c(rep(NA, times = mean(len_NA2)),
                                 zoo::na.locf(voltage_kV_2)),
                UNIT_3 = ifelse(UNIT %in% luanshya_unit_lst,
                                UNIT, NA),
                len_UNIT_2 = length(zoo::na.locf(UNIT_3)),
                len_NA_3 = len_sheet_name - len_UNIT_2,
                UNIT_4 = c(rep(NA, times = mean(len_NA_3)),
                           zoo::na.locf(UNIT_3)),
                SUBSTATION_2c = stringr::str_replace(SUBSTATION_2b,
                                                     "11KV", ""),
                SUBSTATION_2c = stringr::str_replace(SUBSTATION_2c,
                                                     "11kV", ""),
                SUBSTATION_2c = stringr::str_trim(SUBSTATION_2c)
                ) %>% 
  dplyr::select(-len_volt_kV2, -len_NA2, -test_kv, 
                -test_true, -len_sheet_name,
                -len_UNIT_2, -len_NA_3) # -"...1", -"...13"
  
df_luanshya_cleaned
  
unique(df_luanshya_cleaned$...1)
unique(df_luanshya_cleaned$...13)




df_luanshya_cleaned2  <- df_luanshya_cleaned %>%
  dplyr::rename_with(tolower) %>% 
  dplyr::mutate(region = 'luanshya',
                # # division = 'northern'
  )

df_luanshya_cleaned2 
  



# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'luanshya_electricity_substations_data_new_24april',
#                       df_luanshya_cleaned)



# ############################################################################
# 
# df_luanshya_cleaned3 <- df_luanshya_cleaned2 %>%
#     dplyr::rename_with(tolower) %>%
#     dplyr::select(substation = substation_2c, #substation,
#                   unit = unit_4, voltage_kv = voltage_kv_3, #actual_purchase_mwh,
#                   main_mwh, check_mwh, main_mva, check_mva,
#                   variance_share, readings, sheet_year_recoded, file_name,
#                   sheet_name,  sheet_name_recoded, region, division) %>% #"actual_purchase_mwh",
#     dplyr::filter(!is.na(readings)) %>%
#     dplyr::mutate(reading_names = rep(c('pres_mwh', 'prev_mwh', 'adv_mwh'), 
#                                       length(sheet_name)/
#                                         length(c('pres_mwh', 'prev_mwh', 'adv_mwh'))),
#                   main_readings = paste0('main_', reading_names),
#                   check_readings = paste0('check_', reading_names),
#                   ) %>%
#   dplyr::select(-readings) %>%
#   tidyr::pivot_wider(names_from = main_readings,
#                      values_from = main_mwh) %>%
#   tidyr::pivot_wider(names_from = check_readings,
#                      values_from = check_mwh)
# 
# df_luanshya_cleaned3
# colnames(df_luanshya_cleaned3)
# 
# 
# luans_prim_var_lst <- c('substation', 'unit', 'voltage_kv', 
#                         'sheet_year_recoded', 'file_name', 'sheet_name',
#                         'sheet_name_recoded', 'division', 'region')
# 
# df_luan_prim_key <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst)) %>% 
#   # dplyr::select(substation_2c, unit_4, voltage_kv_3, sheet_year_recoded, file_name, sheet_name, sheet_name_recoded, division, region) %>% 
#   dplyr::distinct()
# 
# df_luan_prim_key
# 
# 
# df_luan_main_mva <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), main_mva) %>% 
#   dplyr::filter(!is.na(main_mva))
# 
# df_luan_main_mva
# 
# 
# df_luan_check_mva <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), check_mva) %>% 
#   dplyr::filter(!is.na(check_mva))
# 
# df_luan_check_mva
# 
# 
# df_luan_Main_Pres_MWh <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), main_pres_mwh) %>% 
#   dplyr::filter(!is.na(main_pres_mwh))
# 
# df_luan_Main_Pres_MWh
# 
# 
# df_luan_Main_Prev_MWh <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), main_prev_mwh) %>% 
#   dplyr::filter(!is.na(main_prev_mwh))
# 
# df_luan_Main_Prev_MWh
# 
# df_luan_Main_Adv_MWh <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), main_adv_mwh) %>% 
#   dplyr::filter(!is.na(main_adv_mwh))
# 
# df_luan_Main_Adv_MWh
# 
# 
# 
# df_luan_Check_Pres_MWh <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), check_pres_mwh) %>% 
#   dplyr::filter(!is.na(check_pres_mwh))
# 
# df_luan_Check_Pres_MWh
# 
# 
# df_luan_Check_Prev_MWh <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), check_prev_mwh) %>% 
#   dplyr::filter(!is.na(check_prev_mwh))
# 
# df_luan_Check_Prev_MWh
# 
# df_luan_Check_Adv_MWh <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), check_adv_mwh) %>% 
#   dplyr::filter(!is.na(check_adv_mwh))
# 
# df_luan_Check_Adv_MWh
# 
# 
# ##
# df_luan_main_mva <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), main_mva) %>% 
#   dplyr::filter(!is.na(main_mva))
# 
# df_luan_main_mva
# 
# df_luan_check_mva <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), check_mva) %>% 
#   dplyr::filter(!is.na(check_mva))
# 
# df_luan_check_mva
# 
# df_luan_variance_share <- df_luanshya_cleaned3 %>% 
#   dplyr::select(all_of(luans_prim_var_lst), variance_share) %>% 
#   dplyr::filter(!is.na(variance_share))
# 
# df_luan_variance_share
# 
# 
# 
# 
# df_luanshya_cleaned3 <- dplyr::full_join(df_luan_prim_key,
#                                          df_luan_Main_Pres_MWh,
#                                          by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_Check_Pres_MWh,
#                    by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_Main_Prev_MWh,
#                    by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_Check_Prev_MWh,
#                    by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_Main_Adv_MWh,
#                    by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_Check_Adv_MWh,
#                    by = luans_prim_var_lst) %>%
#   
#   dplyr::full_join(df_luan_main_mva,
#                    by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_check_mva,
#                    by = luans_prim_var_lst) %>% 
#   dplyr::full_join(df_luan_variance_share,
#                    by = luans_prim_var_lst) %>%
#   
#   dplyr::rename(main_consump_mwh = main_adv_mwh,
#                 check_consump_mwh = check_adv_mwh,
#                 # voltage = voltage_kv_3,
#                 year = sheet_year_recoded,
#                 # substation = substation_2c,
#                 # unit = unit_4 
#                 )
# 
# df_luanshya_cleaned3
# 
# table(df_luanshya_cleaned3$year)
# 
# # RSQLite::dbWriteTable(dbConnect(SQLite(),
# #                                 paste0(write_to_db_path,
# #                                        "/",
# #                                        datalake_sqlite)),
# #                       name =  'luanshya_electricity_substations_data_new',
# #                       df_luanshya_cleaned)





######################################### ----- SOUTHERN Division Data ----- #########################################



# Function for collating energy consumption data (southern division load) from the folder
collate_energy_sales_southern_div_data <- function(FolderPathInput){
  
  dataVal_all <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  for (dir in dirs_list){
    # print(dir) ## Directory name
    
    files <- list.files(path = dir,
                        pattern = '.xls')
    # print(files)
    
    for (file in files){
      # print(file)  ## File name
      
      file_path <- paste0(dir, "/", file)
      print(file_path)
      
      sheet_names <- readxl::excel_sheets(path = file_path)
      # print(sheet_names)
      # sheet_names <- sheet_names[!sheet_names %in% 
      #                              c("Sheet1", "Sheet2")] #, "MAY CORRECTED"
      
      for (sheet_name in sheet_names){
        
        print(file_path)
        print(file)
        print(sheet_name)
        
        dataVal <- readxl::read_excel(file_path,
                                      sheet = sheet_name,
                                      skip = 0,
                                      col_names = FALSE) %>%
          dplyr::mutate_all(as.character) %>%
          dplyr::mutate(sheet_name = sheet_name,
                        file_name = file)
        #
        print(head(dataVal))
        print(dim(dataVal))
        # print(dataVal[4, ])
        
        dataVal_all <- dplyr::bind_rows(dataVal, dataVal_all)
        gc()
        
      }
      
    }
    
  }
  gc()
  return(dataVal_all)
  
}

df_energy_sales_south <- collate_energy_sales_southern_div_data(paste0(cig_folder, '/',
                                                                       energy_sales_folder, 
                                                                       '/Energy sales/Southern Division'))
gc()
df_energy_sales_south

# unique(df_energy_sales_south$file_name)
# unique(df_energy_sales_south$sheet_name)
# 
# 
# df_south_b <- df_energy_sales_south %>% 
#   dplyr::select(file_name, sheet_name) %>% 
#   dplyr::distinct() %>% 
#   dplyr::arrange(sheet_name) #file_name
# 
# df_south_b
# sort(unique(df_south_b$sheet_name))





######################################### Southern Data ############################################

df_energy_sales_southern_xlsx_cleaning <- df_energy_sales_south %>%
  dplyr::filter(file_name == "BULK REPORT FOR SOUTHERN REGION 2021.xlsx") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RUARY", " "),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "EMBER", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UARY", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2020 CORRECTED",
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "AMENEDED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISIONAL REVISED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISION",
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded)) #%>%
# dplyr::select(-'...25')


head(df_energy_sales_southern_xlsx_cleaning)


df_southern_cleaning <- df_energy_sales_southern_xlsx_cleaning %>%
  dplyr::rename(SUBSTATION = '...3',
                UNIT_1 = '...4',
                UNIT_2 = '...5',
                PREVIOUS_kWh = '...6',
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12',
                VT_RATIO = '...13',
                CT_RATIO = '...14',
                CONSUMPTION_kWh = '...15',
                MD_KVA = '...16') %>%
  dplyr::mutate(UNIT = ifelse(is.na(UNIT_1) & is.na(UNIT_2),
                                    NA, 
                                    ifelse(is.na(UNIT_1),
                                           UNIT_2,
                                           UNIT_1)))
  
df_southern_cleaning

colnames(df_southern_cleaning)
unique(df_southern_cleaning$sheet_name_recoded)

south_southern_towns <- c('LIVINGSTONE TOWN', 'MAZABUKA TOWN', 'CHOMA TOWN',
                          'SESHEKE/MWANDI', 'MONZE TOWN', 'GWEEMBE',
                          'KALOMO', 'NAMWALA', 'ITT', #'MAAMBA 88/11kV',
                          #'SINAZONGWE  88/33kV', 'PEMBA 11kV OHL'
                          )

df_southern_cleaned <- df_southern_cleaning %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_kWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, sheet_year_recoded,  file_name) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(district = ifelse(SUBSTATION %in% south_southern_towns,
                                  SUBSTATION, NA),
                district = stringr::str_replace(district, ' 11kV OHL',
                                                ''),
                district = stringr::str_replace(district, ' 88/11kV',
                                                ''),
                district = stringr::str_replace(district, '  88/33kV',
                                                ''),
                district = stringr::str_replace(district, ' TOWN',
                                                ''),
                division = 'Southern'
  ) %>%
  dplyr::rename(sheet_name = sheet_name_recoded,
                year = sheet_year_recoded,
                KVA = MD_KVA) %>% 
  dplyr::filter(#!is.na(SUBSTATION),
                !SUBSTATION %in% c('SUBSTATION', 
                                   'BULK METERING FOR SOUTHERN REGION')) %>% 
  dplyr::mutate(len_sheet_name = length(sheet_name),
                len_substr = length(zoo::na.locf(SUBSTATION)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2 = ifelse(len_NA_subst > 0,
                                      c(rep(NA, times = mean(len_NA_subst)),
                                        zoo::na.locf(SUBSTATION)),
                                      zoo::na.locf(SUBSTATION))) %>% 
  dplyr::select(-len_sheet_name, -len_substr, -len_NA_subst) %>% 
  dplyr::filter(!is.na(SUBSTATION_2))


head(df_southern_cleaned)




######################################### Central Data ############################################

df_energy_sales_central_xlsx_cleaning <- df_energy_sales_south %>%
  dplyr::filter(file_name == "BULKY CENTRAL REPORT.xlsx") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2020 CORRECTED",
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "PURCHASES ",
                                                              ""),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "AMENEDED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISIONAL REVISED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISION",
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded)) #%>%
# dplyr::select(-'...25')


head(df_energy_sales_central_xlsx_cleaning)

unique(df_energy_sales_central_xlsx_cleaning$...24)


df_energy_sales_central_cleaning <- df_energy_sales_central_xlsx_cleaning %>% 
  dplyr::select(-'...26', -'...25', -'...24') %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                Import_Export = "...5",
                PREVIOUS_kWh = "...6",
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12',
                VT_RATIO = '...14',
                CT_RATIO = '...13',
                CONSUMPTION_kWh = '...16',
                KVA = '...22',
                MF = "...15",
                # CONSUMPTION_MWh = "...15",
                # MVA = "...44",
                REMARKS = "...23"
  ) #%>%
  # dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_kWh, CONSUMPTION_kWh,
  #               # VOLTAGE_LEVEL, VT_RATIO, CT_RATIO,
  #               KVA, REMARKS,
  #               sheet_name_recoded, sheet_year_recoded, sheet_name, file_name)

df_energy_sales_central_cleaning

df_central_cleaning <- df_energy_sales_central_cleaning

df_central_cleaning

unique(df_central_cleaning$sheet_name_recoded)


south_central_towns <- c('MKUSHI AREA', 'KAPIRI AREA',
                         'SERENJE BRANCH', 'KABWE TOWN')


df_central_cleaned <- df_central_cleaning %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_kWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, KVA,
                sheet_name_recoded, sheet_year_recoded,  file_name) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(district = ifelse(SUBSTATION %in% south_central_towns,
                                  SUBSTATION, NA),
                district = stringr::str_replace(district, ' 11kV OHL',
                                                ''),
                district = stringr::str_replace(district, ' 88/11kV',
                                                ''),
                district = stringr::str_replace(district, '  88/33kV',
                                                ''),
                district = stringr::str_replace(district, ' TOWN',
                                                ''),
                district = stringr::str_replace(district, ' AREA',
                                                ''),
                district = stringr::str_replace(district, ' BRANCH',
                                                ''),
                division = 'Southern'
  ) %>%
  dplyr::rename(sheet_name = sheet_name_recoded,
                year = sheet_year_recoded) %>% 
  dplyr::filter(#!is.na(SUBSTATION),
                !SUBSTATION %in% c('SUBSTATION', 
                                   'BULK METERING FOR CENTRAL REGION')) %>% 
  dplyr::mutate(len_sheet_name = length(sheet_name),
                len_substr = length(zoo::na.locf(SUBSTATION)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2 = ifelse(len_NA_subst > 0,
                                      c(rep(NA, times = mean(len_NA_subst)),
                                        zoo::na.locf(SUBSTATION)),
                                      zoo::na.locf(SUBSTATION))) %>% 
  dplyr::select(-len_sheet_name, -len_substr, -len_NA_subst) %>% 
  dplyr::filter(!is.na(SUBSTATION_2))


head(df_central_cleaned)




######################################### Eastern Data ############################################

df_energy_sales_eastern_xlsx_cleaning <- df_energy_sales_south %>%
  dplyr::filter(file_name == "EASTERN REGION-SD ENERGY REPORT JANUARY 2021.xls") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2020 CORRECTED",
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "AMENEDED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISIONAL REVISED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISION",
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded)) #%>%
# dplyr::select(-'...25')


head(df_energy_sales_eastern_xlsx_cleaning)


unique(df_energy_sales_eastern_xlsx_cleaning$sheet_year_recoded)


df_east_21 <- df_energy_sales_eastern_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c("21")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...6",
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12', 
                VT_RATIO = '...13',
                CT_RATIO = '...14',
                CONSUMPTION_kWh = '...15',
                MD_KVA = '...16'
                # PRESENT_MWh = "...7",
                # CONSUMPTION_MWh = "...15",
                # MVA = "...44",
                # REMARKS = "...45"
                ) %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, sheet_year_recoded, sheet_name, file_name) %>% 
  dplyr::mutate(year = 2021)

df_east_21



df_east_20 <- df_energy_sales_eastern_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c('20')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...6",
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12', 
                VT_RATIO = '...13',
                CT_RATIO = '...14',
                CONSUMPTION_kWh = '...15',
                MD_KVA = '...16'
                # PRESENT_MWh = "...7",
                # CONSUMPTION_MWh = "...15",
                # MVA = "...44",
                # REMARKS = "...45"
  ) %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, sheet_year_recoded, sheet_name, file_name) %>%
  dplyr::mutate(year = 2020)

df_east_20



df_east_2020 <- df_energy_sales_eastern_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c('2020')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...6",
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12', 
                VT_RATIO = '...13',
                CT_RATIO = '...14',
                CONSUMPTION_kWh = '...15',
                MD_KVA = '...16') %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, sheet_year_recoded, sheet_name, file_name) %>%
  dplyr::mutate(year = 2020)

df_east_2020



df_east_2019 <- df_energy_sales_eastern_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c('2019')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...6",
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12', 
                VT_RATIO = '...13',
                CT_RATIO = '...14',
                CONSUMPTION_kWh = '...15',
                MD_KVA = '...16') %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, sheet_year_recoded, sheet_name, file_name) %>%
  dplyr::mutate(year = 2019)

df_east_2019

unique(df_east_2019$SUBSTATION)
unique(df_east_2019$UNIT)
unique(df_east_2019$PREVIOUS_MWh)
unique(df_east_2019$CURRENT_kWh)
unique(df_east_2019$VOLTAGE_LEVEL)
unique(df_east_2019$VT_RATIO)
unique(df_east_2019$CT_RATIO)
unique(df_east_2019$CONSUMPTION_kWh)




df_east_2018 <- df_energy_sales_eastern_xlsx_cleaning %>%
  dplyr::filter(sheet_year_recoded %in% c('2018')) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...6",
                CURRENT_kWh = '...7',
                VOLTAGE_LEVEL = '...12', 
                VT_RATIO = '...13',
                CT_RATIO = '...14',
                CONSUMPTION_kWh = '...15',
                MD_KVA = '...16') %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, sheet_year_recoded, sheet_name, file_name) %>%
  dplyr::mutate(year = 2018)

df_east_2018

unique(df_east_2018$SUBSTATION)
unique(df_east_2018$UNIT)
unique(df_east_2018$PREVIOUS_MWh)
unique(df_east_2018$CURRENT_kWh)
unique(df_east_2018$VOLTAGE_LEVEL)
unique(df_east_2018$VT_RATIO)
unique(df_east_2018$CT_RATIO)
unique(df_east_2018$CONSUMPTION_kWh)



df_eastern_cleaning <- dplyr::bind_rows(df_east_2018, df_east_2019,
                                        df_east_2020, df_east_20, 
                                        df_east_21) #%>%
#   dplyr::rename(SUBSTATION = '...3',
#                 UNIT_1 = '...4',
#                 UNIT_2 = '...5',
#                 PREVIOUS_kWh = '...6',
#                 CURRENT_kWh = '...7',
#                 VOLTAGE_LEVEL = '...12',
#                 VT_RATIO = '...13',
#                 CT_RATIO = '...14',
#                 CONSUMPTION_kWh = '...15',
#                 MD_KVA = '...16') %>%
#   dplyr::mutate(UNIT = ifelse(is.na(UNIT_1) & is.na(UNIT_2),
#                               NA, 
#                               ifelse(is.na(UNIT_1),
#                                      UNIT_2,
#                                      UNIT_1)))

df_eastern_cleaning

unique(df_eastern_cleaning$sheet_name_recoded)


south_eastern_towns <- c('CHIPATA TOWN', 'PETAUKE TOWN',
                         'SINDA TOWN', 'NYIMBA',
                         'KATETE TOWN', 'CHADIZA',
                         'LUANGWA TOWN', 'MFUWE TOWN',
                         'LUNDAZI TOWN', 'CHAMA TOWN')



df_eastern_cleaned <- df_eastern_cleaning %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, CURRENT_kWh, CONSUMPTION_kWh,
                VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, MD_KVA,
                sheet_name_recoded, year,  file_name) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(district = ifelse(SUBSTATION %in% south_eastern_towns,
                                  SUBSTATION, NA),
                district = stringr::str_replace(district, ' 11kV OHL',
                                                ''),
                district = stringr::str_replace(district, ' 88/11kV',
                                                ''),
                district = stringr::str_replace(district, '  88/33kV',
                                                ''),
                district = stringr::str_replace(district, ' TOWN',
                                                ''),
                district = stringr::str_replace(district, ' AREA',
                                                ''),
                district = stringr::str_replace(district, ' BRANCH',
                                                ''),
                sheet_name_recoded = ifelse(sheet_name_recoded == 'JUN2020',
                                  'JUN 2020', sheet_name_recoded),
                division = 'Southern'
  ) %>%
  dplyr::rename(sheet_name = sheet_name_recoded,
                KVA = MD_KVA,
                PREVIOUS_kWh = PREVIOUS_MWh) %>% 
  dplyr::filter(#!is.na(SUBSTATION),
                !SUBSTATION %in% c('SUBSTATION', 
                                   'BULK METERING FOR EASTERN  REGION')) %>% 
  dplyr::mutate(len_sheet_name = length(sheet_name),
                len_substr = length(zoo::na.locf(SUBSTATION)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2 = ifelse(len_NA_subst > 0,
                                      c(rep(NA, times = mean(len_NA_subst)),
                                        zoo::na.locf(SUBSTATION)),
                                      zoo::na.locf(SUBSTATION))) %>% 
  dplyr::select(-len_sheet_name, -len_substr, -len_NA_subst) %>% 
  dplyr::filter(!is.na(SUBSTATION_2))


head(df_eastern_cleaned)





######################################### Western Data ############################################

df_energy_sales_western_xlsx_cleaning <- df_energy_sales_south %>%
  dplyr::filter(file_name == "Western Region Purchases Report for January 2021xlsx.xlsx") %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(sheet_name_recoded = toupper(stringr::str_trim(sheet_name)),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RUARY", ""),
                sheet_name_recoded = stringr::str_replace_all(toupper(sheet_name_recoded),
                                                              pattern = "MARCH", "MAR "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "EMBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "OBER", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UARY", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE-", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JUNE", "JUN"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "JULY", "JUL"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "UST", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2020 CORRECTED",
                                                              "MAY 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGSS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINGS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "MAY 2013 WITH RE-BILLINS",
                                                              "MAY 2013"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "\\(2\\)", ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:punct:]]", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "IL", " "),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "APRI 2020", "APR 2020"),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "RI", "R "),
                
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "AMENEDED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISIONAL REVISED",
                                                              ""),
                sheet_name_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "DIVISION",
                                                              ""),
                sheet_name_recoded = stringr::str_trim(sheet_name_recoded),
                sheet_name_recoded = stringr::str_squish(sheet_name_recoded),
                
                sheet_year_recoded = stringr::str_replace_all(sheet_name_recoded,
                                                              pattern = "[[:alpha:]]", ""),
                sheet_year_recoded = stringr::str_replace_all(sheet_year_recoded, 
                                                              pattern = "[[:space:]]", ""),
                sheet_year_recoded = as.numeric(sheet_year_recoded)) #%>%
# dplyr::select(-'...25')


head(df_energy_sales_western_xlsx_cleaning)


unique(df_energy_sales_western_xlsx_cleaning$sheet_year_recoded)


df_west_col13 <- df_energy_sales_western_xlsx_cleaning %>%
  dplyr::rename(col13 = '...13') %>% 
  dplyr::filter(!is.na(col13)) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) 

unique(df_west_col13$sheet_name_recoded)


df_west_jul_nov_2017 <- df_energy_sales_western_xlsx_cleaning %>% 
  dplyr::filter(sheet_name_recoded %in% c("NOV 2017", "OCT 2017", 
                                          "SEPT 2017", "AUG 2017",
                                          "JUL 2017")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...3",
                UNIT = "...4",
                PREVIOUS_MWh = "...5",
                PRESENT_kWh = '...6',
                # VOLTAGE_LEVEL = '...12', 
                # VT_RATIO = '...13',
                # CT_RATIO = '...14',
                CONSUMPTION_kWh = '...7',
                KVA = '...12',
                # PRESENT_MWh = "...7",
                # CONSUMPTION_MWh = "...15",
                # MVA = "...44",
                REMARKS = "...13"
  ) #%>%
  # dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_kWh, CONSUMPTION_kWh,
  #               # VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, 
  #               MD_KVA, REMARKS,
  #               sheet_name_recoded, sheet_year_recoded, sheet_name, file_name)

df_west_jul_nov_2017

unique(df_west_jul_nov_2017$SUBSTATION)
unique(df_west_jul_nov_2017$UNIT)
unique(df_west_jul_nov_2017$PREVIOUS_MWh)
unique(df_west_jul_nov_2017$PRESENT_kWh)
# unique(df_west_jul_nov_2017$VOLTAGE_LEVEL)
# unique(df_west_jul_nov_2017$VT_RATIO)
unique(df_west_jul_nov_2017$KVA)
unique(df_west_jul_nov_2017$CONSUMPTION_kWh)


df_west_col11 <- df_energy_sales_western_xlsx_cleaning %>%
  dplyr::rename(col11 = '...11') %>%
  dplyr::filter(!is.na(col11)) %>%
  # dplyr::filter(!sheet_name_recoded %in% c("NOV 2017", "OCT 2017", 
  #                                         "SEPT 2017", "AUG 2017",
  #                                         "JUL 2017")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) 

df_west_col11

unique(df_west_col11$sheet_name_recoded)



df_west_other <- df_energy_sales_western_xlsx_cleaning %>% 
  dplyr::filter(!sheet_name_recoded %in% c("NOV 2017", "OCT 2017", 
                                          "SEPT 2017", "AUG 2017",
                                          "JUL 2017")) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::rename(SUBSTATION = "...1",
                UNIT = "...2",
                PREVIOUS_MWh = "...3",
                PRESENT_kWh = '...4',
                # VOLTAGE_LEVEL = '...12', 
                # VT_RATIO = '...13',
                # CT_RATIO = '...14',
                CONSUMPTION_kWh = '...5',
                KVA = '...6',
                # PRESENT_MWh = "...7",
                # CONSUMPTION_MWh = "...15",
                # MVA = "...44",
                REMARKS = "...7"
  ) %>%
dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_kWh, CONSUMPTION_kWh,
              # VOLTAGE_LEVEL, VT_RATIO, CT_RATIO,
              KVA, REMARKS,
              sheet_name_recoded, sheet_year_recoded, sheet_name, file_name)

df_west_other

unique(df_west_other$SUBSTATION)
unique(df_west_other$UNIT)
unique(df_west_other$PREVIOUS_MWh)
unique(df_west_other$PRESENT_kWh)
# unique(df_west_jul_nov_2017$VOLTAGE_LEVEL)
# unique(df_west_jul_nov_2017$VT_RATIO)
unique(df_west_other$KVA)
unique(df_west_other$CONSUMPTION_kWh)




df_western_cleaning <- dplyr::bind_rows(df_west_other,
                                        df_west_jul_nov_2017)

df_western_cleaning



unique(df_western_cleaning$sheet_name_recoded)


south_western_towns <- c('MONGU', 'KALABO',
                         'SENANGA', 'SIOMA',
                         'KAOMA', 'LUKULU',
                         'SHANGOMBO- DIESEL')


df_western_cleaned <- df_western_cleaning %>%
  dplyr::select(SUBSTATION, UNIT, PREVIOUS_MWh, PRESENT_kWh, CONSUMPTION_kWh,
                # VOLTAGE_LEVEL, VT_RATIO, CT_RATIO, 
                KVA, sheet_name_recoded, sheet_year_recoded, file_name) %>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ),
    # -sheet_name
  ) %>%
  dplyr::mutate(district = ifelse(SUBSTATION %in% south_western_towns,
                                  SUBSTATION, NA),
                district = stringr::str_replace(district, ' 11kV OHL',
                                                ''),
                district = stringr::str_replace(district, ' 88/11kV',
                                                ''),
                district = stringr::str_replace(district, '  88/33kV',
                                                ''),
                district = stringr::str_replace(district, ' TOWN',
                                                ''),
                district = stringr::str_replace(district, ' AREA',
                                                ''),
                district = stringr::str_replace(district, ' BRANCH',
                                                ''),
                sheet_name_recoded = ifelse(sheet_name_recoded == "AUG",
                                            'AUG 2018', sheet_name_recoded),
                division = 'Southern'
  ) %>%
  dplyr::rename(sheet_name = sheet_name_recoded,
                #KVA = MD_KVA,
                year = sheet_year_recoded,
                PREVIOUS_kWh = PREVIOUS_MWh,
                CURRENT_kWh = PRESENT_kWh) %>% 
  dplyr::filter(#!is.na(SUBSTATION),
                !SUBSTATION %in% c('SUBSTATION', 
                                   'MONTHLY READINGS',
                                   'BULK METERING FOR WESTERN REGION')) %>% 
  dplyr::mutate(len_sheet_name = length(sheet_name),
                len_substr = length(zoo::na.locf(SUBSTATION)),
                len_NA_subst = len_sheet_name - len_substr,
                SUBSTATION_2 = ifelse(len_NA_subst > 0,
                                      c(rep(NA, times = mean(len_NA_subst)),
                                        zoo::na.locf(SUBSTATION)),
                                      zoo::na.locf(SUBSTATION))) %>% 
  dplyr::select(-len_sheet_name, -len_substr, -len_NA_subst) %>% 
  dplyr::filter(!is.na(SUBSTATION_2))


head(df_western_cleaned)




df_southern_div <- dplyr::bind_rows(df_western_cleaned, df_southern_cleaned,
                                    df_eastern_cleaned, df_central_cleaned)

df_southern_div


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'southern_division_electricity_substations_data',
#                       df_southern_div)



### This is the end of this script ####


