### This script is for processing the datalake data ###

#Load packages

library(tidyverse)
library(RSQLite)

#####

combined_db_folder <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data"  # provide the path where the datalake is stored
combined_sqlite <- "combined_data.sqlite" # this is the name of the main (initial) datalake database
data_lake2_sqlite <- "data_lake2.sqlite" # this is the name of the second datalake database

final_sqlite <- "final_db.sqlite" # this is the name of the final database

#### ---- Processing the data ---- ####

## combined
db_conn_combined <- DBI::dbConnect(RSQLite::SQLite(),
                          dbname = paste0(combined_db_folder,
                                          "/", combined_sqlite))
db_conn_combined

db_comb_tbls_combined <- DBI::dbListTables(db_conn_combined)
db_comb_tbls_combined




clean_tbl_lst <- c("bulk_supply_point_loadings", "connections", "consumption_all",
                   "substation_map_data", "transmission_electricity_substations_data",
                   "generation_10_year_inflows_discharge_data",
                   "generation_10_year_inflows_discharge_generation_data",
                   "generation_2020_actual_generation_kpi_data", 
                   "generation_2020_actual_transmission_kpi_data",
                   "generation_projects_database_gfz_data",
                   "generation_zesco_ipp_hourly_data")



# # Function for copying data from the database to another database
# copy_clean_db_data_to_another_db <- function(dbPathInput, dbNameInput, dbNameOutput){
#   
#   db_conn <- dbConnect(SQLite(),
#                        paste0(dbPathInput, "/", dbNameInput))
#   
#   db_tbls <- dbListTables(db_conn)
#   
#   dataVal_all <- NULL
#   
#   for (db_tbl in db_tbls){
#     
#     print(db_tbl)  ## print table name
#     
#     dataVal <- RSQLite::dbReadTable(db_conn, db_tbl)
#     
#     # RSQLite::dbWriteTable(dbConnect(SQLite(),
#     #                                 paste0(dbPathInput,
#     #                                        "/",
#     #                                        dbNameOutput)),
#     #                       name =  db_tbl,
#     #                       dataVal %>%
#     #                         dplyr::as_tibble())
#     
#     gc()
#     
#   }
#   
#   dbDisconnect(db_conn)
#   gc()
#   
# }
# 
# copy_clean_db_data_to_another_db(write_to_combined_db, combined_sqlite, data_lake)
# 



## note: all the sub_station data needs further processing and some of the generation data (for generation files, a different script will be used)

get_minor_tbl_data_processing <- function(InputDBPath, InputTblList, 
                                          InputDBWriteFolder, InputDBWrite){
  
  tbl_lst <- InputTblList
  
  for (tbl in tbl_lst){
    
    print(tbl)
    
    if(tbl == 'bulk_supply_point_loadings'){
      
      df_data <- dplyr::tbl(InputDBPath, 'bulk_supply_point_loadings') %>% 
        dplyr::mutate(hourly_int = paste0(hour_start, '-', hour_end)) %>% 
        # dplyr::select(-date, -hour_start, -hyphen, -hour_end) %>% 
        dplyr::select("date_char", "hourly_int", "lusaka_mw", "copperebelt_mw",
                      "southern_mw", "northern_mw", "cec_mw", "kansanshi_mw",
                      "kalumbila_mw", "lumwana_mw", "chambishi copper smelter_mw",
                      "nwec - kalumbila and lumwana_mw", "kafue gorge upper_mw",
                      "kariba complex_mw", "choma_mw", "kafue town_mw", 
                      "kabwe town_mw", "kapiri mposhi_mw", "kasama_mw",
                      "livingstone_mw", "mazabuka_mw", "msoro_mw", "ndola_mw",
                      "sesheke_mw", "solwezi_mw", "demand and losses_mw",
                      "generation_mw" )
        # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
    }
    else
      if(tbl == 'connections'){
      
      df_data <- dplyr::tbl(InputDBPath, 'connections')
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
    }
    else
      if(tbl == 'consumption_all'){
      
      df_data <- dplyr::tbl(InputDBPath, 'consumption_all') %>% 
        dplyr::select("town", "msno", "tariff", "year", "jan", "feb", "mar",
                      "apr", "may", "jun", "jul", "aug", "sept", "oct",
                      "nov", "dec", "file_name", "sheet_name") #%>% 
        # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else
      if(tbl == 'substation_map_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'substation_map_data') #%>% 
        # dplyr::select("town", "msno", "tariff", "year", "jan", "feb", "mar",
        #               "apr", "may", "jun", "jul", "aug", "sept", "oct",
        #               "nov", "dec", "file_name", "sheet_name") %>% 
        # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
    } else
      if(tbl == 'transmission_electricity_substations_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'transmission_electricity_substations_data') %>% 
        dplyr::select(dates_char = "Dates_char", "HoD", "datapoints", 
                      "common_name", "source_file", "line_to_subst", 
                      capacity_kV = "voltage_kV",
                      "MW", "MVAr", "MVA", "kV") %>%
        dplyr::arrange(datapoints, common_name, dates_char, HoD)
        # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else
      if(tbl == 'generation_10_year_inflows_discharge_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'generation_10_year_inflows_discharge_data') %>% 
        dplyr::select(dates_char = dates, "plant_name", "inflow_m3_s",
                      "outflow_m3_s", "act_energy_mwh", "pot_energy_mwh",
                      "file_name", "sheet_name") %>%
        dplyr::arrange(plant_name)
        # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else
      if(tbl == 'generation_10_year_inflows_discharge_generation_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'generation_10_year_inflows_discharge_generation_data') %>% 
        dplyr::select("plant_name", "year", "jan", "feb", "mar", "apr", "may",
                      "jun", "jul", "aug", "sep", "oct", "nov", "dec",
                      "variable", "sheet_name", "file_name") %>%
        dplyr::arrange(plant_name)
      # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else
      if(tbl == 'generation_2020_actual_generation_kpi_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'generation_2020_actual_generation_kpi_data') %>% 
        dplyr::select("region", "station", "availability", "planned_outage",
                      "planned_hours", "unplanned_outage", "unplanned_hours",      
                      "file_name", "sheet_name") %>%
        dplyr::arrange(region, station)
      # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else
      if(tbl == 'generation_2020_actual_transmission_kpi_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'generation_2020_actual_transmission_kpi_data') %>% 
        dplyr::select(region, capacity_volt = voltage, availability,
                      planned_outage, planned_hours, unplanned_outage,
                      unplanned_hours, file_name, sheet_name) %>%
        dplyr::arrange(region, capacity_volt)
      # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else
      if(tbl == 'generation_projects_database_gfz_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'generation_projects_database_gfz_data') %>% 
        dplyr::select(-series) %>%
        dplyr::arrange(proj_name)
      # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
    } 
    else 
      if(tbl == 'generation_zesco_ipp_hourly_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'generation_zesco_ipp_hourly_data') %>%
        dplyr::arrange(dates_2) %>% 
        dplyr::as_tibble() %>% 
        dplyr::mutate(start_hour = stringr::str_replace(start_hour, "\\:00:00", ""),
                      start_hour = stringr::str_replace(start_hour, dates_2, ""),
                      start_hour = as.numeric(start_hour),
                      end_hour = stringr::str_replace(end_hour, "\\:00:00", ""),
                      end_hour = stringr::str_replace(end_hour, dates_2, ""),
                      end_hour = as.numeric(end_hour),
                      hourly_int = paste0(start_hour, '-', end_hour)
                      ) %>% 
        dplyr::select(dates_char = dates_2, hourly_int, kgps_mw, kariba_mw,
                      vic_falls_mw, bangweulu_mw, ngonye_mw, itpc_mw, mcl_mw,
                      musonda_mw, lusiwasi_mw, lunzua_mw, ndola_energy_ph2_mw,
                      ndola_energy_ph1_mw, file_name, sheet_name)
      # colnames()
      
      # print(df_data)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(InputDBWriteFolder,
                                             "/",
                                             InputDBWrite)),
                            name = tbl,
                            df_data %>%
                              dplyr::as_tibble())
      
      }
    else{
      
      print(paste0('This Table ', tbl, ' was not processed.'))
      
    }
    
  }
  
  RSQLite::dbDisconnect()

}

# get_minor_tbl_data_processing(db_conn_combined, clean_tbl_lst,
#                               combined_db_folder, data_lake2_sqlite)

# get_minor_tbl_data_processing(db_conn_combined, "connections",
#                               combined_db_folder, data_lake2_sqlite)



### Functions to process other tables

not_clean_tbl_lst <- dplyr::setdiff(db_comb_tbls_combined, clean_tbl_lst)
not_clean_tbl_lst


get_major_tbl_data_processing <- function(InputDBPath, InputTblList, 
                                          InputDBWriteFolder, InputDBWrite){
  
  tbl_lst <- InputTblList
  tbl_lst <- tbl_lst[stringr::str_detect(tbl_lst, "_substations_")]
  
  df_copperbelt <- NULL
  df_northern <- NULL
  df_lusaka <- NULL
  df_lusaka_loads <- NULL
  df_southern <- NULL
  
  for (tbl in tbl_lst){
    
    print(tbl)
    
    if(tbl == 'copperbelt_electricity_substations_data'){
      
      df_data <- dplyr::tbl(InputDBPath, 'copperbelt_electricity_substations_data') %>% 
        dplyr::rename_with(tolower) %>% 
        dplyr::select(substation = substation_2, feeder, 
                      previous_mwh = main_previous_mwh,
                      present_mwh = main_present_mwh, 
                      consump_mwh = main_advance, check_previous_mwh,
                      check_present_mwh, check_consump_mwh = check_advance, 
                      md_mw, file_name, sheet_name_recoded) %>% 
        dplyr::rename(sheet_name = sheet_name_recoded) %>% 
        dplyr::mutate(division = 'copperbelt') %>% 
        dplyr::as_tibble()
      
      # print(df_data)
      
      df_copperbelt <- dplyr::bind_rows(df_copperbelt, df_data)
      
      # print(df_data %>% 
      #         colnames())
      
    }
    else
      if(tbl == 'luapula_electricity_substations_data'){

        df_data <- dplyr::tbl(InputDBPath, 'luapula_electricity_substations_data') %>%
          dplyr::rename_with(tolower) %>% 
        dplyr::select(substation = substation_2a, unit,
                      voltage_kv, previous_mwh,
                      present_mwh, consump_mwh = consumption_mwh, mva,
                      sheet_year_recoded, file_name, 
                      sheet_name_recoded, remarks ) %>%
          dplyr::rename(sheet_name = sheet_name_recoded,
                        year = sheet_year_recoded) %>% 
          dplyr::mutate(division = 'northern') %>% 
          dplyr::as_tibble()

        # print(df_data)
        
        df_northern <- dplyr::bind_rows(df_northern, df_data)
        
        # print(df_data %>%
        #         colnames())

      }
    else
        if(tbl == 'ndola_electricity_substations_data'){

              df_data <- dplyr::tbl(InputDBPath, 'ndola_electricity_substations_data') %>%
                dplyr::rename_with(tolower) %>%
                dplyr::select(substation = substation_2a, unit, voltage_kv,
                              previous_mwh, present_mwh,
                              consump_mwh = consumption_mwh,
                              mva, mw, sheet_year_recoded,
                              file_name, sheet_name_recoded, remarks) %>%
                dplyr::rename(sheet_name = sheet_name_recoded,
                              year = sheet_year_recoded) %>%
                dplyr::mutate(division = 'northern') %>% 
                dplyr::as_tibble()
              
              # print(df_data)
              
              df_northern <- dplyr::bind_rows(df_northern, df_data)

              # print(df_data %>%
              #         colnames())

        }
    else
      if(tbl == 'northern_electricity_substations_data'){
        
        df_data <- dplyr::tbl(InputDBPath, 'northern_electricity_substations_data') %>%
          dplyr::rename_with(tolower) %>%
          dplyr::select(substation = substation_2a, unit, 
                        voltage_kv, previous_mwh, present_mwh,
                        consump_mwh = consumption_mwh,
                        mva, sheet_year_recoded,
                        file_name, sheet_name_recoded, remarks) %>%
          dplyr::rename(sheet_name = sheet_name_recoded,
                        year = sheet_year_recoded) %>%
          dplyr::mutate(division = 'northern') %>% 
          dplyr::as_tibble()
        
        # print(df_data)
        
        df_northern <- dplyr::bind_rows(df_northern, df_data)
        
        # print(df_data %>%
        #         colnames())

      }
    else
      if(tbl == 'lusaka_electricity_substations_data'){

        df_data <- dplyr::tbl(InputDBPath, 'lusaka_electricity_substations_data') %>%
          dplyr::rename_with(tolower) %>%
          dplyr::select(substation = substation_name,
                        unit, voltage_kv, previous_kwh,
                        present_kwh, consumption_kwh,
                        kva, file_name, sheet_name_recoded, remarks ) %>%
          dplyr::mutate(present_kwh = present_kwh/1000,
                        previous_kwh = previous_kwh/1000,
                        consumption_kwh = consumption_kwh/1000,
                        kva = kva/1000) %>% 
          dplyr::rename(sheet_name = sheet_name_recoded,
                        mva = kva,
                        present_mwh = present_kwh,
                        previous_mwh = previous_kwh,
                        consump_mwh = consumption_kwh) %>%
          dplyr::mutate(division = 'lusaka') %>% 
          dplyr::as_tibble()
        
        # print(df_data)
        
        df_lusaka <- dplyr::bind_rows(df_lusaka, df_data)
        
        # print(df_data %>%
        #         colnames())

      }
    else
      if(tbl == 'lusaka_electricity_substations_loads_data'){

        df_data <- dplyr::tbl(InputDBPath, 'lusaka_electricity_substations_loads_data') %>%
          dplyr::rename_with(tolower) %>%
          dplyr::select(substation, unit, previous_mwh, present_mwh,
                        consump_mwh = advance, mf, 
                        consump_adj_mwh = actual, mva, 
                        year, file_name, sheet_name) %>%
          dplyr::mutate(division = 'lusaka') %>% 
          dplyr::as_tibble()
        
        # print(df_data)
        
        df_lusaka_loads <- dplyr::bind_rows(df_lusaka_loads, df_data)
        
        # print(df_data %>%
        #         colnames())
        
      }
    else
      if(tbl == 'southern_division_electricity_substations_data'){

        df_data <- dplyr::tbl(InputDBPath, 'southern_division_electricity_substations_data') %>%
          dplyr::rename_with(tolower) %>%
          dplyr::mutate(current_kwh = current_kwh/1000,
                        previous_kwh = previous_kwh/1000,
                        consumption_kwh = consumption_kwh/1000,
                        kva = kva/1000) %>%
          dplyr::rename(mva = kva,
                        present_mwh = current_kwh,
                        previous_mwh = previous_kwh,
                        consump_mwh = consumption_kwh) %>%
          dplyr::select(substation = substation_2, unit, 
                        voltage_kv = voltage_level, 
                        previous_mwh, present_mwh,
                        consump_mwh, mva, year, file_name, sheet_name,
                        division, district, vt_ratio, ct_ratio) %>%
          dplyr::mutate(division = 'southern') %>% 
          dplyr::as_tibble()
        
        # print(df_data)
        
        df_southern <- dplyr::bind_rows(df_southern, df_data)
        
        # print(df_data %>%
        #         colnames())

      }
    else{
      
      print(paste0('This Table ', tbl, ' was not processed.'))
      
    }
    
  }
  
  
  print(df_copperbelt)
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = "copperbelt_div_elec_substations_data",
                        df_copperbelt %>%
                          dplyr::as_tibble())
  
  print(df_northern)
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = "northern_1_div_elec_substations_data",
                        df_northern %>%
                          dplyr::as_tibble())
  
  print(df_lusaka)
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = "lusaka_div_elec_substations_data",
                        df_lusaka %>%
                          dplyr::as_tibble())
  
  print(df_lusaka_loads)
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = "lusaka_div_elec_substations_loads_data",
                        df_lusaka_loads %>%
                          dplyr::as_tibble())
  
  print(df_southern)
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = "southern_div_elec_substations_data",
                        df_southern %>%
                          dplyr::as_tibble())
  
}

get_major_tbl_data_processing(db_conn_combined, not_clean_tbl_lst,
                              combined_db_folder, data_lake2_sqlite)



################ Luanshya and Muchinga regions ---------------------------------

get_major_lm_tbl_data_processing <- function(InputDBPath, InputTblList, 
                                          InputDBWriteFolder, InputDBWrite){
  
  tbl_lst <- InputTblList
  
  df_northern <- NULL
  
  for (tbl in tbl_lst){
    
    # print(tbl)
    
    if(tbl == 'luanshya_electricity_substations_data_new_24april'){
      
      df_data <- dplyr::tbl(InputDBPath, 
                            'luanshya_electricity_substations_data_new_24april') %>%
        dplyr::rename_with(tolower) %>%
        dplyr::select(substation = substation_2c,
                      unit = unit_4, voltage_kv = voltage_kv_3,
                      main_mwh, check_mwh, main_mva, check_mva,
                      variance_share, readings, sheet_year_recoded, file_name,
                      sheet_name,  sheet_name_recoded, division) %>%
        dplyr::as_tibble() %>% 
        dplyr::filter(!is.na(readings)) %>%
        dplyr::mutate(reading_names = rep(c('pres_mwh', 'prev_mwh', 'adv_mwh'), 
                                          length(sheet_name)/
                                            length(c('pres_mwh', 'prev_mwh', 'adv_mwh'))),
                      main_readings = paste0('main_', reading_names),
                      check_readings = paste0('check_', reading_names),
                      region = 'luanshya') %>%
        dplyr::select(-readings) %>%
        tidyr::pivot_wider(names_from = main_readings,
                           values_from = main_mwh) %>%
        tidyr::pivot_wider(names_from = check_readings,
                           values_from = check_mwh)
      
      luans_prim_var_lst <- c('substation', 'unit', 'voltage_kv', 
                              'sheet_year_recoded', 'file_name', 'sheet_name',
                              'sheet_name_recoded', 'division', 'region')
      
      df_data_prim_key <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst)) %>% 
        dplyr::distinct()

      df_data_main_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_mva) %>% 
        dplyr::filter(!is.na(main_mva))

      df_data_check_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_mva) %>% 
        dplyr::filter(!is.na(check_mva))
      
      df_data_Main_Pres_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_pres_mwh) %>% 
        dplyr::filter(!is.na(main_pres_mwh))

      df_data_Main_Prev_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_prev_mwh) %>% 
        dplyr::filter(!is.na(main_prev_mwh))

      df_data_Main_Adv_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_adv_mwh) %>% 
        dplyr::filter(!is.na(main_adv_mwh))
      
      df_data_Check_Pres_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_pres_mwh) %>% 
        dplyr::filter(!is.na(check_pres_mwh))
      
      df_data_Check_Prev_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_prev_mwh) %>% 
        dplyr::filter(!is.na(check_prev_mwh))
      
      df_data_Check_Adv_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_adv_mwh) %>% 
        dplyr::filter(!is.na(check_adv_mwh))
      
      df_data_main_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_mva) %>% 
        dplyr::filter(!is.na(main_mva))
      
      df_data_check_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_mva) %>% 
        dplyr::filter(!is.na(check_mva))
      
      df_data_variance_share <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), variance_share) %>% 
        dplyr::filter(!is.na(variance_share))
      
      df_data <- dplyr::full_join(df_data_prim_key,
                                  df_data_Main_Pres_MWh,
                                  by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Check_Pres_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Main_Prev_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Check_Prev_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Main_Adv_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Check_Adv_MWh,
                         by = luans_prim_var_lst) %>%
        dplyr::full_join(df_data_main_mva,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_check_mva,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_variance_share,
                         by = luans_prim_var_lst) %>%
        dplyr::rename(main_consump_mwh = main_adv_mwh,
                      check_consump_mwh = check_adv_mwh,
                      year = sheet_year_recoded
                      )
      
      # print(df_data)
      
      df_northern <- dplyr::bind_rows(df_northern, df_data)
      
      print(table(df_data$year))
      
      # print(df_data %>% 
      #         colnames())
      
    } 
    else
    if(tbl == 'muchinga_electricity_substations_data_new_24april'){
      
      df_data <- dplyr::tbl(InputDBPath, 
                            'muchinga_electricity_substations_data_new_24april') %>%
        dplyr::rename_with(tolower) %>%
        dplyr::select(substation = substation_2c,
                      unit = unit_4, voltage_kv = voltage_kv_3,
                      main_mwh, check_mwh, main_mva, check_mva,
                      variance_share, readings, sheet_year_recoded, file_name,
                      sheet_name,  sheet_name_recoded, division) %>%
        dplyr::as_tibble() %>% 
        dplyr::filter(!is.na(readings)) %>%
        dplyr::mutate(reading_names = rep(c('pres_mwh', 'prev_mwh', 'adv_mwh'), 
                                          length(sheet_name)/
                                            length(c('pres_mwh', 'prev_mwh', 'adv_mwh'))),
                      main_readings = paste0('main_', reading_names),
                      check_readings = paste0('check_', reading_names),
                      region = 'luanshya') %>%
        dplyr::select(-readings) %>%
        tidyr::pivot_wider(names_from = main_readings,
                           values_from = main_mwh) %>%
        tidyr::pivot_wider(names_from = check_readings,
                           values_from = check_mwh)
      
      luans_prim_var_lst <- c('substation', 'unit', 'voltage_kv', 
                              'sheet_year_recoded', 'file_name', 'sheet_name',
                              'sheet_name_recoded', 'division', 'region')
      
      df_data_prim_key <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst)) %>% 
        dplyr::distinct()
      
      df_data_main_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_mva) %>% 
        dplyr::filter(!is.na(main_mva))
      
      df_data_check_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_mva) %>% 
        dplyr::filter(!is.na(check_mva))
      
      df_data_Main_Pres_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_pres_mwh) %>% 
        dplyr::filter(!is.na(main_pres_mwh))
      
      df_data_Main_Prev_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_prev_mwh) %>% 
        dplyr::filter(!is.na(main_prev_mwh))
      
      df_data_Main_Adv_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_adv_mwh) %>% 
        dplyr::filter(!is.na(main_adv_mwh))
      
      df_data_Check_Pres_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_pres_mwh) %>% 
        dplyr::filter(!is.na(check_pres_mwh))
      
      df_data_Check_Prev_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_prev_mwh) %>% 
        dplyr::filter(!is.na(check_prev_mwh))
      
      df_data_Check_Adv_MWh <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_adv_mwh) %>% 
        dplyr::filter(!is.na(check_adv_mwh))
      
      df_data_main_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), main_mva) %>% 
        dplyr::filter(!is.na(main_mva))
      
      df_data_check_mva <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), check_mva) %>% 
        dplyr::filter(!is.na(check_mva))
      
      df_data_variance_share <- df_data %>% 
        dplyr::select(all_of(luans_prim_var_lst), variance_share) %>% 
        dplyr::filter(!is.na(variance_share))
      
      df_data <- dplyr::full_join(df_data_prim_key,
                                  df_data_Main_Pres_MWh,
                                  by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Check_Pres_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Main_Prev_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Check_Prev_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Main_Adv_MWh,
                         by = luans_prim_var_lst) %>% 
        dplyr::full_join(df_data_Check_Adv_MWh,
                         by = luans_prim_var_lst) %>%
        dplyr::full_join(df_data_main_mva,
                         by = luans_prim_var_lst) %>% 
        
        ### These two dataframes below were not included -----------------------
      
        # dplyr::full_join(df_data_check_mva,
        #                  by = luans_prim_var_lst) %>% 
        # dplyr::full_join(df_data_variance_share,
        #                  by = luans_prim_var_lst) %>%
        
        dplyr::rename(main_consump_mwh = main_adv_mwh,
                      check_consump_mwh = check_adv_mwh,
                      year = sheet_year_recoded
                      )

      # print(df_data)
      
      df_northern <- dplyr::bind_rows(df_northern, df_data)
      
      print(table(df_data$year))

      # print(df_data %>%
      #         colnames())
      
    }
    else{
      
      print(paste0('This Table ', tbl, ' was not processed.'))
      
    }
    
  }
  
  # print(df_northern)
  print(table(df_northern$year))
  
  RSQLite::dbWriteTable(dbConnect(SQLite(),
                                  paste0(InputDBWriteFolder,
                                         "/",
                                         InputDBWrite)),
                        name = "northern_2_div_elec_substations_data",
                        df_northern %>%
                          dplyr::as_tibble())
  
}

not_clean_lm_tbl_lst <- c("luanshya_electricity_substations_data_new_24april",
                          "muchinga_electricity_substations_data_new_24april")

get_major_lm_tbl_data_processing(db_conn_combined, not_clean_lm_tbl_lst,
                              combined_db_folder, data_lake2_sqlite)





## ---- copying data between databases ---- ##

# This is a function for copying data from the second datalake database to the final database
copy_db_data_to_another_db <- function(dbPathInput, dbNameInput, dbNameOutput){
  
  db_conn <- dbConnect(SQLite(),
                       paste0(dbPathInput, "/", dbNameInput))
  
  db_tbls <- dbListTables(db_conn)
  
  dataVal_all <- NULL
  
  for (db_tbl in db_tbls){
    
    print(db_tbl)  ## print table name
    
    dataVal <- RSQLite::dbReadTable(db_conn, db_tbl)
    
    RSQLite::dbWriteTable(dbConnect(SQLite(),
                                    paste0(dbPathInput,
                                           "/",
                                           dbNameOutput)),
                          name =  db_tbl,
                          dataVal %>%
                            dplyr::as_tibble(),
                          overwrite = TRUE)
    
    gc()
    
  }
  
  dbDisconnect(db_conn)
  
  gc()
  
}

copy_db_data_to_another_db(combined_db_folder, data_lake2_sqlite, final_sqlite)

  
### ------------------ This is the end of this script ------------------ ####


