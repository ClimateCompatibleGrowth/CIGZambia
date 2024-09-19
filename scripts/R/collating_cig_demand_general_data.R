### This script is for collating slightly manipulated (in Excel) data for bulk supply loadings and substations maps from the project folder (E014.3_IRP) to a datalake database ###

#Load packages
library(tidyverse)
library(zoo)
library(RSQLite)


## Point the software where to look (you will have to specify on your computer)
bsp_file <- "./data/2020 Bulk Supply Point Loadings BSP_adjusted.xlsx" # This data is first manipulated in Excel before reading it into R (check the data folder)
substation_map <- "./data/Substation Data Map_adjusted.xlsx" # This data is first manipulated in Excel before reading it into R (check the data folder)

write_to_db_path <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # this folder where data will be stored (you have to create this folder)
datalake_sqlite <- "combined_data.sqlite" # this is the name of the files (datalake database)


## Steps for collating bulk supply loadings data from the files (this raw data the Excel manipulated data can be found in the transmission folder)
df_bsp_loadings <- readxl::read_excel(bsp_file,
                                      sheet = "Sheet1",
                                      skip = 0)%>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ), 
    -"...7"
  ) %>%
  dplyr::filter(!is.na(Hour_End)) %>%
  dplyr::rename_all(., .funs = tolower) %>% 
  dplyr::mutate(hour_start = lubridate::hour(hour_start),
                hour_end = lubridate::hour(hour_end),
                #file_name = file,
                #sheet_name = sheet_name,
                date = zoo::na.locf(date),
                date_char = as.character(date))


df_bsp_loadings

colnames(df_bsp_loadings)

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'bulk_supply_point_loadings',
#                       df_bsp_loadings)





## Steps for collating substation map data from the files (this raw data the Excel manipulated data can be found in the transmission folder)
df_substation_map <- readxl::read_excel(substation_map,
                                        sheet = "Map",
                                        skip = 0)%>%
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    ), 
    #-"...7"
  ) %>%
  dplyr::filter(!is.na(substation)) %>%
  dplyr::rename_all(., .funs = tolower) %>% 
  dplyr::mutate(across(where(is.numeric),
                       as.character)) %>% 
  tidyr::pivot_longer(cols = starts_with('xxx '),
                      names_to = 'variable',
                      values_to = 'datapoints') %>% 
  dplyr::filter(!is.na(datapoints)) %>% 
  dplyr::mutate(variable = stringr::str_replace(variable,
                                                "xxx ",
                                                ""))

df_substation_map


df_substation_map2 <- df_substation_map %>% 
  dplyr::bind_cols(as.data.frame(stringr::str_split_fixed(
    df_substation_map$variable, " ", 2)) %>%
      dplyr::rename(voltage_kv = V1) %>%
      dplyr::select(-V2) %>%
      dplyr::as_tibble()) %>% 
  dplyr::mutate(voltage_kv = stringr::str_replace(voltage_kv,
                                                "kv",
                                                "")) %>% 
  dplyr::select(substation, substation_abbr, code1,
                code2, datapoints, voltage_kv)


df_substation_map2

## Get some quick insights 
sort(unique(df_substation_map2$substation))
sort(unique(df_substation_map2$substation_abbr))
length(unique(df_substation_map2$substation_abbr))
sort(unique(df_substation_map2$voltage_kv))


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_db_path,
#                                        "/",
#                                        datalake_sqlite)),
#                       name =  'substation_map_data',
#                       df_substation_map2)




### This is the end of this script ####
