### This script is for extract the relevant generations data from the generation datalake database ###

#Load packages
library(dplyr)

## Point the software where to look (you will have to specify on your computer)
write_to_generation_db <- "/media/tembo/Seagate Portable Drive/Consulting Work/CIG/work/cigzambia_output/data/generation_data"  # this is where generation datalake is stored 
write_to_combined_db <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # this is the folder where the main datalake is stored

combined_sqlite <- "combined_data.sqlite"  # main datalake
generat_sqlite <- "generation_data.sqlite"  # generation datalake


#### ---- Processing the data ---- ####

db_generat <- DBI::dbConnect(RSQLite::SQLite(),
                             dbname = paste0(write_to_generation_db,
                                             "/", generat_sqlite))
db_generat

db_generat_tbls <- DBI::dbListTables(db_generat)
db_generat_tbls


### testing

# # df_testing <- DBI::dbReadTable(db_generat,
# #                                    'generation_raw_xlsx_data')
# 
# df_testing <- tbl(db_generat, 'generation_raw_xlsx_data') %>%
#   dplyr::select(-folder_path) %>% 
#   dplyr::filter(file_name == "ZESCO Gen & IPPs - June 2020 - June 2021-Hourly MW.xlsx")
# 
# df_testing


start_time <- Sys.time()

df_testing <- tbl(db_generat, 'generation_raw_xlsx_data') %>%
  dplyr::select(-folder_path, -file_ext) %>% 
  dplyr::filter(file_name == "ZESCO Gen & IPPs - June 2020 - June 2021-Hourly MW.xlsx") %>% 
  dplyr::distinct() %>% 
  dplyr::as_tibble()

df_testing

end_time <- Sys.time()
time_needed <- end_time - start_time
print(time_needed)

unique(df_testing$variable)
unique(df_testing$value)




df_zesco_ipps2 <- df_testing %>% 
  dplyr::select(-file_name) %>% 
  dplyr::filter(variable != 'sheet_name') %>% 
  tidyr::pivot_wider(names_from = 'variable',
                     values_from = 'value') %>%
  tidyr::unnest('KGPS MW') %>% 
  tidyr::unnest('Kariba MW')

df_zesco_ipps2
colnames(df_zesco_ipps2)

tidyr::unnest(df_zesco_ipps2, 'KGPS MW')

tidyr::un


###
get_hourly_ipp_data <- function(dbPathInput){
  
  # dataVal_raw <- InputData
  
  dataVal_raw <- RSQLite::dbReadTable(dbPathInput,
                                      'generation_raw_xlsx_data')
  
  dataVal <- dataVal_raw %>%
    dplyr::filter(file_name == "ZESCO Gen & IPPs - June 2020 - June 2021-Hourly MW.xlsx") %>% 
    # dplyr::as_tibble() %>% 
    dplyr::select(
      where(
        ~sum(!is.na(.x)) > 0
      )
    )# %>% dplyr::as_tibble()
  
  uniq_dates <- unique(dataVal$"...618")
  # print(len_dates)
  # 
  # df_dates <- dplyr::bind_cols(dates = rep(uniq_dates))
  
  print(sort(colnames(dataVal)))
  
  gc()
  
  dataVal <- dataVal %>%
    dplyr::rename(start_hour = "...2", end_hour = "...4",
                  
                # kgps_mw = "KGPS MW", kariba_mw = "Kariba MW",
                # vic_falls_mw = "Vic Falls", bangweulu_mw = "Bangweulu MW",
                # ngonye_mw = "Ngonye MW", itpc_mw = "ITPC MW",
                # mcl_mw = "MCL MW", musonda_mw = "Musonda MW",
                # lusiwasi_mw = "Lusiwasi MW", lunzua_mw = "Lunzua MW",
                # ndola_energy_ph2_mw = "Ndolla Energy Ph 2 MW",
                # ndola_energy_ph1_mw = "Ndola Energy Ph1",
                
                kgps_mw = "KGPS.MW", kariba_mw = "Kariba.MW",
                vic_falls_mw = "Vic.Falls", bangweulu_mw = "Bangweulu.MW",
                ngonye_mw = "Ngonye.MW", itpc_mw = "ITPC.MW",
                mcl_mw = "MCL.MW", musonda_mw = "Musonda.MW",
                lusiwasi_mw = "Lusiwasi.MW", lunzua_mw = "Lunzua.MW",
                ndola_energy_ph2_mw = "Ndolla.Energy.Ph.2.MW",
                ndola_energy_ph1_mw = "Ndola.Energy.Ph1",
                
                dates = "...618") %>%
    # dplyr::select(-"...3", -"...6", -"...9", -"...10", -"...11", -"...15",
    #               -"...17", -"...21",  -"...23", -"...27",  -"...29", -"34.4",
    #               -"...620", -"...624", -"...628", -"...632",  -"...635"
    #               ) %>%
    dplyr::select("dates", "start_hour", "end_hour", "kgps_mw",
                  "kariba_mw", "vic_falls_mw", "bangweulu_mw", "ngonye_mw",
                  "itpc_mw", "mcl_mw", "musonda_mw", "lusiwasi_mw",
                  "lunzua_mw", "ndola_energy_ph2_mw", "ndola_energy_ph1_mw",
                  "sheet_name", "file_name") %>%
    dplyr::filter(!is.na(start_hour) &
                    !is.na(end_hour)) %>%
    dplyr::mutate(len_sheet_name = length(sheet_name),
                  len_dates = length(zoo::na.locf(dates)),
                  len_NA_dates = len_sheet_name - len_dates,
                  dates_2 = ifelse(len_NA_dates > 0,
                                   c(rep(NA, times = mean(len_NA_dates)),
                                     zoo::na.locf(dates)),
                                   zoo::na.locf(dates)),
                  dates = dates_2,
                  dates_alt = as.Date(start_hour),
                  dates_test = dates == dates_alt,
                  start_hour = lubridate::hour(lubridate::ydm_hms(start_hour)),
                  end_hour = lubridate::hour(lubridate::ydm_hms(end_hour))) %>%
    dplyr::select(-len_sheet_name, -len_dates, -len_NA_dates,
                  -dates_2, -dates_test)



  uniq_dates <- unique(dataVal$dates)
  # print(uniq_dates)

  print(paste0(
    as.POSIXct(paste0(min(uniq_dates), " 00:00:00")),
    " -- ",
    as.POSIXct(paste0(max(uniq_dates), " 23:00:00")))
    )

  seq_val <- seq(as.POSIXct(paste0(min(uniq_dates), " 00:00:00")),
                 as.POSIXct(paste0(max(uniq_dates), " 23:00:00")),
                 by = "hour")

  df_date <- dplyr::bind_cols(date_time = seq_val) %>%
    dplyr::mutate(dates2 = as.Date(date_time),
                  start_hour2 = lubridate::hour(lubridate::ydm_hms(date_time)),
                  end_hour2 = lubridate::hour(lubridate::ydm_hms(date_time)) + 1,
                  end_hour2 = ifelse(end_hour2 == 24, 0, end_hour2))
  
  print(dim(df_date))
  print(df_date)

  # print(seq_val)

  print(paste0(min(uniq_dates), " -- ", max(uniq_dates)))

  dataVal <- dplyr::bind_cols(dataVal, df_date) %>%
    dplyr::mutate(start_hour = start_hour2,
                  end_hour = end_hour2) %>% 
    dplyr::select(-end_hour2, -start_hour2, -dates2)
  
}

gc()
df_zesco_ipps <- get_hourly_ipp_data(db_generat)
df_zesco_ipps

# df_zesco_ipps2 <- get_hourly_ipp_data(df_gene_data2)
# df_zesco_ipps2


# df_zesco_ipps == df_zesco_ipps2

colnames(df_zesco_ipps)

table(df_zesco_ipps$...3, df_zesco_ipps$...6)
table(df_zesco_ipps$...635)
table(df_zesco_ipps$dates_test)


gc()

# rm(df_zesco_ipps_hourly)
df_zesco_ipps_hourly <- df_gene_data2 %>%
  dplyr::filter(file_name == "ZESCO Gen & IPPs - June 2020 - June 2021-Hourly MW.xlsx") %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::select("...607", "...2", "...4",  "KGPS MW",  "Kariba MW",
                "Vic Falls", "Bangweulu MW", "Ngonye MW", "ITPC MW",
                "MCL MW", "Musonda MW", "Lusiwasi MW", "Lunzua MW",
                "Ndolla Energy Ph 2 MW", "Ndola Energy Ph1",
                "sheet_name", "file_name") %>% 
  dplyr::rename(dates = "...607", start_hour = "...2", end_hour = "...4",
                kgps_mw = "KGPS MW", kariba_mw = "Kariba MW",
                vic_falls_mw = "Vic Falls", bangweulu_mw = "Bangweulu MW",
                ngonye_mw = "Ngonye MW", itpc_mw = "ITPC MW",
                mcl_mw = "MCL MW", musonda_mw = "Musonda MW",
                lusiwasi_mw = "Lusiwasi MW", lunzua_mw = "Lunzua MW",
                ndola_energy_ph2_mw = "Ndolla Energy Ph 2 MW",
                ndola_energy_ph1_mw = "Ndola Energy Ph1") %>% 
  dplyr::filter(!is.na(start_hour) &
                  !is.na(end_hour)) %>% 
  dplyr::mutate(len_sheet_name = length(sheet_name),
                len_dates = length(zoo::na.locf(dates)),
                len_NA_dates = len_sheet_name - len_dates,
                dates_2 = ifelse(len_NA_dates > 0,
                                      c(rep(NA, times = mean(len_NA_dates)),
                                        zoo::na.locf(dates)),
                                      zoo::na.locf(dates))) %>% 
  dplyr::select(-len_sheet_name, -len_dates, -len_NA_dates)

df_zesco_ipps_hourly

colnames(df_zesco_ipps_hourly)


colnames(df_zesco_ipps_hourly)
unique(df_zesco_ipps_hourly$sheet_name)


dfxxx = df_zesco_ipps_hourly %>% 
  select(dates, start_hour, end_hour, dates_2)

dfxxx



# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_zesco_ipp_hourly_data',
#                       df_zesco_ipps_hourly)

rm(df_zesco_ipps_hourly)


make_string_dates <- function(InputYears, InputMonths){
  
  dates_val <- NULL
  
  for (i in 2010:2020){
    
    for (j in 1:12) {
      
      print(i)
      
      print(j)
      
      date_char <- ifelse(j < 10,
                          paste0('0', j, '-', i),
                          paste0(j, '-', i))
      
      print(date_char)
      
      # dates_val <- dplyr::bind_rows(dates_val, date_char)
      dates_val <- c(dates_val, date_char)
      
    }
    
  }
  
  return(dates_val)
  
}
dates_lst <- make_string_dates(2010:2020, 1:12)
# 
# for (i in 2010:2020){
#   
#   for (j in 1:12) {
#     
#     print(i)
#     
#     print(j)
#     
#     date_char <- ifelse(i < 10,
#            paste0('0', j, '-', i),
#            paste0(j, '-', i))
#     
#     print(date_char)
#     
#   }
#   
# }


df_10_year_inflows_discharge <- df_gene_data2 %>%
  dplyr::filter(file_name == "10 -Year inflow,discharge.xlsx") %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::filter(sheet_name == 'SUMMARY') %>% 
  # dplyr::select("...607", "...2", "...4",  "KGPS MW",  "Kariba MW",
  #               "Vic Falls", "Bangweulu MW", "Ngonye MW", "ITPC MW",
  #               "MCL MW", "Musonda MW", "Lusiwasi MW", "Lunzua MW",
  #               "Ndolla Energy Ph 2 MW", "Ndola Energy Ph1",
  #               "sheet_name", "file_name") %>% 
  dplyr::rename(itezhi_inflow_m3_s = '...2', itezhi_outflow_m3_s = '...3',
                itezhi_act_energy_mwh = '...4', itezhi_pot_energy_mwh = '...5',
                kgu_inflow_m3_s = '...8', kgu_outflow_m3_s = '...9',
                kgu_act_energy_mwh = '...10', kgu_pot_energy_mwh = '...11',
                vic_falls_inflow_m3_s = '...14', vic_falls_outflow_m3_s = '...15',
                vic_falls_act_energy_mwh = '...16', vic_falls_pot_energy_mwh = '...17',
                kariba_north_inflow_m3_s = '...20', kariba_north_outflow_m3_s = '...21',
                kariba_north_act_energy_mwh = '...22', kariba_north_pot_energy_mwh = '...23',
                lunzua_inflow_m3_s = '...26', lunzua_outflow_m3_s = '...27',
                lunzua_act_energy_mwh = '...28', lunzua_pot_energy_mwh = '...29',
                lusiwasi_inflow_m3_s = '...32', lusiwasi_outflow_m3_s = '...33',
                lusiwasi_act_energy_mwh = '...34', lusiwasi_pot_energy_mwh = '...35',
                musonda_falls_inflow_m3_s = '...38', musonda_falls_outflow_m3_s = '...39',
                musonda_falls_act_energy_mwh = '...40', musonda_falls_pot_energy_mwh = '...41',
                chishimba_falls_inflow_m3_s = '...44', chishimba_falls_outflow_m3_s = '...45',
                chishimba_falls_act_energy_mwh = '...46', chishimba_falls_pot_energy_mwh = '...47',
                shiwangandu_inflow_m3_s = '...50', shiwangandu_outflow_m3_s = '...51',
                shiwangandu_act_energy_mwh = '...52', shiwangandu_pot_energy_mwh = '...53',
                date_col = "ITEZHI-TEZHI PS") %>%
  dplyr::filter(date_col != 'Date',
                !is.na(date_col)) %>%
  dplyr::mutate(dates = dates_lst) %>%
  dplyr::select("dates", "itezhi_inflow_m3_s", "itezhi_outflow_m3_s",
                "itezhi_act_energy_mwh", "itezhi_pot_energy_mwh",
                "kgu_inflow_m3_s", "kgu_outflow_m3_s", "kgu_act_energy_mwh",
                "kgu_pot_energy_mwh", "vic_falls_inflow_m3_s",
                "vic_falls_outflow_m3_s", "vic_falls_act_energy_mwh",
                "vic_falls_pot_energy_mwh", "kariba_north_inflow_m3_s",
                "kariba_north_outflow_m3_s", "kariba_north_act_energy_mwh",
                "kariba_north_pot_energy_mwh", "lunzua_inflow_m3_s",
                "lunzua_outflow_m3_s", "lunzua_act_energy_mwh",
                "lunzua_pot_energy_mwh", "lusiwasi_inflow_m3_s",
                "lusiwasi_outflow_m3_s", "lusiwasi_act_energy_mwh",
                "lusiwasi_pot_energy_mwh", "musonda_falls_inflow_m3_s",
                "musonda_falls_outflow_m3_s", "musonda_falls_act_energy_mwh",
                "musonda_falls_pot_energy_mwh", "chishimba_falls_inflow_m3_s",
                "chishimba_falls_outflow_m3_s", "chishimba_falls_act_energy_mwh",
                "chishimba_falls_pot_energy_mwh", "shiwangandu_inflow_m3_s",
                "shiwangandu_outflow_m3_s", "shiwangandu_act_energy_mwh",
                "shiwangandu_pot_energy_mwh", "sheet_name", "file_name") %>% 
  tidyr::pivot_longer(cols = !c(dates, sheet_name, file_name)) %>% 
  
  dplyr::mutate(plant_name = ifelse(stringr::str_detect(name, 'itezhi'),
                                    'Itezhi-Tezhi', name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'kgu'),
                                    'Kafue Gorge', plant_name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'vic_falls'),
                                    'Victoria Falls', plant_name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'kariba_north'),
                                    'Kariba North', plant_name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'lunzua'),
                                    'Lunzua', plant_name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'lusiwasi'),
                                    'Lusiwasi', plant_name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'musonda_falls'),
                                    'Musonda Falls', plant_name),
                
                plant_name = ifelse(stringr::str_detect(plant_name, 'chishimba_falls'),
                                    'Chishimba Falls', plant_name),
                plant_name = ifelse(stringr::str_detect(plant_name, 'shiwangandu'),
                                    'Shiwangandu', plant_name),
                
                plant_variable = stringr::str_replace(name, 'itezhi_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'kgu_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'vic_falls_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'kariba_north_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'lunzua_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'lusiwasi_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'musonda_falls_', ''),
                
                plant_variable = stringr::str_replace(plant_variable, 'chishimba_falls_', ''),
                plant_variable = stringr::str_replace(plant_variable, 'shiwangandu_', ''),
                
                value = as.numeric(value)
                ) %>% 
  dplyr::select(-name) %>% 
  tidyr::pivot_wider(names_from = plant_variable,
                     values_from = value) %>% 
  dplyr::arrange(plant_name)

df_10_year_inflows_discharge

colnames(df_10_year_inflows_discharge)
unique(df_10_year_inflows_discharge$date_col)

colnames(df_10_year_inflows_discharge)
unique(df_10_year_inflows_discharge$sheet_name)


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_10_year_inflows_discharge_data',
#                       df_10_year_inflows_discharge)



plt1 <- dplyr::full_join(df_10_year_inflows_discharge,
                 df_10_year_inflows_discharge %>% 
                   dplyr::group_by(plant_name) %>% 
                   dplyr::summarise(outflow_max = max(outflow_m3_s, na.rm = TRUE),
                                    act_energy_max = max(act_energy_mwh, na.rm = T)),
                 by = c('plant_name')) %>% 
  dplyr::mutate(outflow_index = outflow_m3_s/outflow_max,
                act_energy_index = act_energy_mwh/act_energy_max) %>% 
  dplyr::select(-sheet_name, -file_name) %>% 
  
  
  ggplot2::ggplot() +
  # geom_point(aes(x = outflow_index,
  #                y = act_energy_index,
  #                colour = plant_name))
  geom_line(aes(x = outflow_index,
                y = act_energy_index,
                colour = plant_name))

plt1


## Just checking
plt2 <- df_10_year_inflows_discharge %>% 
  dplyr::group_by(dates, plant_name) %>% 
  dplyr::summarise(act_energy_max = sum(act_energy_mwh, na.rm = T)) %>% 
  dplyr::mutate(dates = paste0('01-', dates),
                dates = lubridate::dmy(dates)) %>% 
  
  ggplot2::ggplot() +
  geom_line(aes(x = dates,
                y = act_energy_max,
                colour = plant_name))

plt2


plt3 <- dplyr::full_join(df_10_year_inflows_discharge,
                         df_10_year_inflows_discharge %>% 
                           dplyr::group_by(plant_name) %>% 
                           dplyr::summarise(act_energy_max = max(act_energy_mwh, na.rm = T)),
                         by = c('plant_name')) %>% 
  dplyr::mutate(act_energy_index = act_energy_mwh/act_energy_max) %>% 
  # df_10_year_inflows_discharge %>% 
  # dplyr::group_by(dates, plant_name) %>% 
  # dplyr::summarise(act_energy_max = sum(act_energy_mwh, na.rm = T)) %>% 
  dplyr::mutate(dates = paste0('01-', dates),
                dates = lubridate::dmy(dates)) %>% 
  
  ggplot2::ggplot() +
  geom_line(aes(x = dates,
                y = act_energy_index,
                colour = plant_name))

plt3

rm(df_10_year_inflows_discharge)
rm(dfxxx)
rm(plt1)
rm(plt2)
rm(plt3)


var_type_lst <- rep(c(rep("inflow_m3_s", length(2010:2020)),
                      rep("turbine_discharge_m3_s", length(2010:2020)),
                      rep("power_generation_mw", 
                          length(2010:2020))), 9)

df_10_year_inflows_discharge_main <- df_gene_data2 %>%
  dplyr::filter(file_name == "10 -Year inflow,discharge.xlsx") %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::filter(sheet_name != 'SUMMARY') %>% 
  dplyr::rename(jan = '...2', feb = '...3',
                mar = '...4', apr = '...5',
                may = '...6', jul = '...8',
                aug = '...9', sep = '...10',
                oct = '...11', nov = '...12',
                jun = '...57', dec = '...58',
                year_itezhi = "KAFUE RIVER @ KAFUE HOOK BRIDGE -  AVERAGE MONTHLY INFLOW",
                year_kafue = "KAFUE RIVER @ NYIMBA -  AVERAGE MONTHLY INFLOW",
                year_vic_kariba = "ZAMBEZI RIVER @ NANA'S FARM -  AVERAGE MONTHLY INFLOW (m3/s)",
                # year_kariba = "ZAMBEZI RIVER @ NANA'S FARM -  AVERAGE MONTHLY INFLOW (m3/s)",
                year_lunzua = "LUNZUA RIVER -  AVERAGE MONTHLY INFLOW",
                year_lusiwasi = "LUSIWASI RIVER @ MASASE -  AVERAGE MONTHLY INFLOW",
                year_musonda = "LUONGO RIVER @ MWENDA KASHIBA -  AVERAGE MONTHLY INFLOW",
                year_chishimba = "LUOMBE RIVER @ KASAMA-MPOROKOSO ROAD BRIDGE -  AVERAGE MONTHLY INFLOW",
                year_shiwangandu = "MANSHYA RIVER -  AVERAGE MONTHLY INFLOW") %>%
  # dplyr::select(year, jan, feb, mar, apr, may, 
  #               jun, jul, aug, sep, oct, nov, dec,
  #               sheet_name, file_name) %>% 
  dplyr::mutate(across(!c('sheet_name', 'file_name'), as.numeric),
                # # year = max(year_itezhi, year_shiwangandu, na.rm = TRUE),
                # year = sum(c_across(year_itezhi:year_shiwangandu), na.rm = TRUE),
                plant_name = stringr::str_replace(sheet_name, ' PS', '')) %>%
  rowwise() %>%
  dplyr::mutate(#across(!c('sheet_name', 'file_name'), as.numeric),
  #        plant_name = stringr::str_replace(sheet_name, ' PS', ''),
         year = max(c(year_itezhi, year_kafue,# year_vic, 
                      year_vic_kariba,
                      year_lunzua, year_lusiwasi, year_musonda,
                      year_chishimba, year_shiwangandu),
                    na.rm = TRUE),
         year2 = max(c_across(year_itezhi:year_shiwangandu), na.rm = TRUE)) %>%
  # # dplyr::select(year, jan, feb, mar, apr, may, 
  # #               jun, jul, aug, sep, oct, nov, dec,
  # #               sheet_name, file_name) %>% 
  dplyr::filter(!is.na(year),
                !is.infinite(year)) %>%
  dplyr::select(#year_itezhi, year_kafue, 
                year, # year2,#
                jan, feb, mar, apr, may, 
                jun, jul, aug, sep, oct, nov, dec,
                plant_name, sheet_name, file_name) %>%
  dplyr::bind_cols(variable = var_type_lst)


df_10_year_inflows_discharge_main


unique(df_10_year_inflows_discharge_main$sheet_name)
table(df_10_year_inflows_discharge_main$sheet_name)
table(df_10_year_inflows_discharge_main$year)

length(unique(df_10_year_inflows_discharge_main$sheet_name))
length(unique(df_10_year_inflows_discharge_main$plant_name))


length(unique(df_gene_data2$sheet_name))

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_10_year_inflows_discharge_generation_data',
#                       df_10_year_inflows_discharge_main)



df_2020_trans_gene_kpis <- df_gene_data2 %>%
  dplyr::filter(file_name == "2020 Actual Transmission Generation KPI.xlsx",
                sheet_name %in% c("January 2020", "February 2020",
                                  "March 2020", "April 2020", "May 2020",
                                  "June 2020", "July 2020", "August 2020",
                                  "September 2020", " October 2020",
                                  "November 2020", "December 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  )


df_2020_trans_gene_kpis


unique(df_2020_trans_gene_kpis$sheet_name)
table(df_2020_trans_gene_kpis$sheet_name)


voltage_type <- c("330kV", "220kV", "132kV", "88kV", "66kV", "33kV", "330/220 kV",
                  "330/132 kV", "330/88 kV",  "330/66 kV",  "330/33 kV",  "220/88 kV",
                  "220/66kV", "220/33 kV", "220/11 kV", "132/33 kV", "132/11 KV", 
                  "88/66 kV", "88/33 kV", "88/11 kV", "66/33 kV", "66/11 kV" )



## January
df_2020_trans_gene_kpis_jan <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("January 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...85',
                key_var2 = '...8'
                )

df_2020_trans_gene_kpis_jan


df_2020_trans_gene_kpis_jan_stations <- df_2020_trans_gene_kpis_jan %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...87'
    ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                 value, NA),
                other_vars = ifelse(variable_name != 'station',
                                              value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                 c(rep(NA, times = mean(len_NA_stationXX)),
                                   zoo::na.locf(stationXX)),
                                 zoo::na.locf(stationXX))
                ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                             "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)
  

df_2020_trans_gene_kpis_jan_stations


df_2020_trans_gene_kpis_jan_volts <- df_2020_trans_gene_kpis_jan %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                     c(rep(NA, times = mean(len_NA_voltageXX)),
                                       zoo::na.locf(voltageXX)),
                                     zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_jan_volts



## February
df_2020_trans_gene_kpis_feb <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("February 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...91',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_jan


df_2020_trans_gene_kpis_feb_stations <- df_2020_trans_gene_kpis_feb %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...93'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_feb_stations



df_2020_trans_gene_kpis_feb_volts <- df_2020_trans_gene_kpis_feb %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_feb_volts


## March
df_2020_trans_gene_kpis_mar <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("March 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...96',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_mar


df_2020_trans_gene_kpis_mar_stations <- df_2020_trans_gene_kpis_mar %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...98'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_mar_stations




df_2020_trans_gene_kpis_mar_volts <- df_2020_trans_gene_kpis_mar %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_mar_volts





## April
df_2020_trans_gene_kpis_apr <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("April 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...105',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_apr


df_2020_trans_gene_kpis_apr_stations <- df_2020_trans_gene_kpis_apr %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...107'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_apr_stations




df_2020_trans_gene_kpis_apr_volts <- df_2020_trans_gene_kpis_apr %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_apr_volts




## May
df_2020_trans_gene_kpis_may <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("May 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...110',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_may


df_2020_trans_gene_kpis_may_stations <- df_2020_trans_gene_kpis_may %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...112'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_may_stations





df_2020_trans_gene_kpis_may_volts <- df_2020_trans_gene_kpis_may %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_may_volts



## June
df_2020_trans_gene_kpis_jun <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("June 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...115',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_jun


df_2020_trans_gene_kpis_jun_stations <- df_2020_trans_gene_kpis_jun %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...117'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_jun_stations




df_2020_trans_gene_kpis_jun_volts <- df_2020_trans_gene_kpis_jun %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_jun_volts


## July
df_2020_trans_gene_kpis_jul <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("July 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...123',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_jul


df_2020_trans_gene_kpis_jul_stations <- df_2020_trans_gene_kpis_jul %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...125'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_jul_stations





df_2020_trans_gene_kpis_jul_volts <- df_2020_trans_gene_kpis_jul %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_jul_volts


## August
df_2020_trans_gene_kpis_aug <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("August 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...128',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_aug


df_2020_trans_gene_kpis_aug_stations <- df_2020_trans_gene_kpis_aug %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...130'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_aug_stations




df_2020_trans_gene_kpis_aug_volts <- df_2020_trans_gene_kpis_aug %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_aug_volts




## September
df_2020_trans_gene_kpis_sep <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("September 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...133',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_sep


df_2020_trans_gene_kpis_sep_stations <- df_2020_trans_gene_kpis_sep %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...135'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_sep_stations





df_2020_trans_gene_kpis_sep_volts <- df_2020_trans_gene_kpis_sep %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_sep_volts


## October
df_2020_trans_gene_kpis_oct <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c(" October 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...141',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_oct


df_2020_trans_gene_kpis_oct_stations <- df_2020_trans_gene_kpis_oct %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...143'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS"))%>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_oct_stations




df_2020_trans_gene_kpis_oct_volts <- df_2020_trans_gene_kpis_oct %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_oct_volts




## November
df_2020_trans_gene_kpis_nov <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("November 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...146',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_nov


df_2020_trans_gene_kpis_nov_stations <- df_2020_trans_gene_kpis_nov %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...148'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS")) %>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value),
                # stationXX_3 = ifelse(stationXX_2 == 'Lusiwasi' &
                #                        value == 60.45,
                #                      'Chishimba Falls',
                #                      stationXX_2)
                ) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_nov_stations




df_2020_trans_gene_kpis_nov_volts <- df_2020_trans_gene_kpis_nov %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_nov_volts


## December
df_2020_trans_gene_kpis_dec <- df_2020_trans_gene_kpis %>% 
  dplyr::filter(sheet_name %in% c("December 2020")) %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(key_var1 = '...151',
                key_var2 = '...8'
  )

df_2020_trans_gene_kpis_dec


df_2020_trans_gene_kpis_dec_stations <- df_2020_trans_gene_kpis_dec %>% 
  dplyr::filter(key_var1 %in% c('Kafue Gorge', 'Kariba North', 
                                'Kariba North Ext', 'Victoria Falls',
                                'Musonda Falls', 'Lunzua',
                                'Lusiwasi', 'Chishimba Falls',
                                'Shiwangandu', 'Station',
                                'Lusiwasi Upper')) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                
                station_south = key_var1, station_north = key_var2,
                
                avail_north = '...9', planned_outage_north = '...10',
                planned_hours_north = '...11', unplanned_outage_north = '...12',
                unplanned_hours_north = '...153'
  ) %>% 
  dplyr::select(station_south, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, station_north, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                stationXX = ifelse(variable_name == 'station',
                                   value, NA),
                other_vars = ifelse(variable_name != 'station',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_stationXX = length(zoo::na.locf(stationXX)),
                len_NA_stationXX = len_sheet_name - len_stationXX,
                stationXX_2 = ifelse(len_NA_stationXX > 0,
                                     c(rep(NA, times = mean(len_NA_stationXX)),
                                       zoo::na.locf(stationXX)),
                                     zoo::na.locf(stationXX))
  ) %>% 
  dplyr::filter(!value %in% c("Station", "% AVAILABILITY", "PLANNED Outage",
                              "UNPLANNED Outage", "TOTALS")) %>% 
  # dplyr::select(-name) %>% 
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -stationXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, stationXX_2, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::filter(!stationXX_2 %in% c('TOTALS')) %>% 
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(station = stationXX_2,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_dec_stations




df_2020_trans_gene_kpis_dec_volts <- df_2020_trans_gene_kpis_dec %>% 
  dplyr::filter(key_var1 %in% voltage_type) %>% 
  dplyr::rename(avail_south = '...2', planned_outage_south = '...3',
                planned_hours_south = '...4', unplanned_outage_south = '...5',
                unplanned_hours_south = '...6',
                voltage = key_var1,
                avail_north = key_var2, planned_outage_north = '...9',
                planned_hours_north = '...10', unplanned_outage_north = '...11',
                unplanned_hours_north = '...12'
  ) %>% 
  dplyr::select(voltage, avail_south, planned_outage_south,
                planned_hours_south, unplanned_outage_south,
                unplanned_hours_south, avail_north, 
                planned_outage_north, planned_hours_north,
                unplanned_outage_north, unplanned_hours_north,
                sheet_name, file_name) %>% 
  tidyr::pivot_longer(cols = !c(sheet_name, file_name)) %>% 
  dplyr::mutate(variable_name = stringr::str_replace(name, '_south', ''),
                variable_name = stringr::str_replace(variable_name, '_north', ''),
                region = ifelse(stringr::str_detect(name, 'north'),
                                'North', name),
                region = ifelse(stringr::str_detect(name, 'south'),
                                'South', region),
                voltageXX = ifelse(variable_name == 'voltage',
                                   value, NA),
                other_vars = ifelse(variable_name != 'voltage',
                                    value, NA),
                len_sheet_name = length(sheet_name),
                len_voltageXX = length(zoo::na.locf(voltageXX)),
                len_NA_voltageXX = len_sheet_name - len_voltageXX,
                voltage_kV = ifelse(len_NA_voltageXX > 0,
                                    c(rep(NA, times = mean(len_NA_voltageXX)),
                                      zoo::na.locf(voltageXX)),
                                    zoo::na.locf(voltageXX))
  ) %>%
  dplyr::mutate(value = as.numeric(value)) %>%
  dplyr::filter(!is.na(value)) %>% 
  dplyr::select(-name, -voltageXX, -other_vars) %>%
  tidyr::pivot_wider(names_from = variable_name,
                     values_from = value) %>% 
  dplyr::select(region, voltage_kV, avail, planned_outage, planned_hours,
                unplanned_outage, unplanned_hours, sheet_name, file_name) %>%
  dplyr::mutate(avail = avail/100) %>%
  dplyr::rename(voltage = voltage_kV,
                availability = avail) %>% 
  dplyr::arrange(region)


df_2020_trans_gene_kpis_dec_volts



df_2020_trans_gene_kpis_stations_cleaned <- dplyr::bind_rows(df_2020_trans_gene_kpis_jan_stations,
                                                             df_2020_trans_gene_kpis_feb_stations,
                                                             df_2020_trans_gene_kpis_mar_stations,
                                                             df_2020_trans_gene_kpis_apr_stations,
                                                             df_2020_trans_gene_kpis_may_stations,
                                                             df_2020_trans_gene_kpis_jun_stations,
                                                             df_2020_trans_gene_kpis_jul_stations,
                                                             df_2020_trans_gene_kpis_aug_stations,
                                                             df_2020_trans_gene_kpis_sep_stations,
                                                             df_2020_trans_gene_kpis_oct_stations,
                                                             df_2020_trans_gene_kpis_nov_stations,
                                                             df_2020_trans_gene_kpis_dec_stations) %>% 
  dplyr::mutate(sheet_name = stringr::str_trim(sheet_name))

df_2020_trans_gene_kpis_stations_cleaned



df_2020_trans_gene_kpis_volts_cleaned <- dplyr::bind_rows(df_2020_trans_gene_kpis_jan_volts,
                                                             df_2020_trans_gene_kpis_feb_volts,
                                                             df_2020_trans_gene_kpis_mar_volts,
                                                             df_2020_trans_gene_kpis_apr_volts,
                                                             df_2020_trans_gene_kpis_may_volts,
                                                             df_2020_trans_gene_kpis_jun_volts,
                                                             df_2020_trans_gene_kpis_jul_volts,
                                                             df_2020_trans_gene_kpis_aug_volts,
                                                             df_2020_trans_gene_kpis_sep_volts,
                                                             df_2020_trans_gene_kpis_oct_volts,
                                                             df_2020_trans_gene_kpis_nov_volts,
                                                             df_2020_trans_gene_kpis_dec_volts) %>% 
  dplyr::mutate(sheet_name = stringr::str_trim(sheet_name),
                voltage = stringr::str_replace(tolower(voltage),
                                               'kv', ''),
                voltage = stringr::str_trim(voltage),
                voltage = paste0(voltage, ' kV'))

df_2020_trans_gene_kpis_volts_cleaned



# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_2020_actual_generation_kpi_data',
#                       df_2020_trans_gene_kpis_stations_cleaned)
# 
# 
# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_2020_actual_transmission_kpi_data',
#                       df_2020_trans_gene_kpis_volts_cleaned)



###################

change_ticks <- function(InputData){
  
  sym_val <- ifelse(InputData == '√√',
                    'Completed',
                    InputData)
  
  sym_val <- ifelse(InputData == '√',
                    'On-going or preparedy',
                    sym_val)
  
  sym_val <- ifelse(InputData == 'xxx',
                    'Commited and construction on-going',
                    sym_val)
  
  sym_val <- ifelse(InputData == 'xx',
                    'Committed financing only',
                    sym_val)
  
  sym_val <- ifelse(InputData == 'x',
                    'Committed Directive',
                    sym_val)
  
  sym_val <- ifelse(InputData == 'y',
                    'Yes and sufficient information',
                    sym_val)
  
  # √ - On-going or preparedy, xxx - Commited and construction on-going, xx - Committed financing only, 
  # x - Committed Directive, y - Yes and sufficient information
  
  
}

df_gfz_projects_database <- df_gene_data2 %>%
  dplyr::filter(file_name == "Generation-Projects-Database-GFZ-27042020 (1).xlsx") %>% 
  dplyr::select(
    where(
      ~sum(!is.na(.x)) > 0
    )
  ) %>% 
  dplyr::rename(proj_name = 'Project Name', series = 'No.',
                cap_mw = "Capacity \r\n[MW]", 
                annual_avg_energy_gwh = "Annual Average Energy [GWh]",
                cap_factor = "Capacity Factor",
                location = Location, technology = Technology,
                overnight_cost_usd_mil = "Overnight Investment cost, Million USD",
                ref_document = "Reference Document",
                # planned_hours_north = '...10', 
                invest_cost_usd_per_kw = "Specific investment cost USD per kW",
                status_pre_feas = Status,
                status_feas = '...9',
                status_committed = '...10',
                status_candidate = '...11'
  ) %>% 
  dplyr::filter(!is.na(series)) %>% 
  dplyr::select(series, proj_name, cap_mw, annual_avg_energy_gwh, cap_factor,
                location, technology, status_pre_feas, status_feas,
                status_committed, status_candidate, overnight_cost_usd_mil,
                invest_cost_usd_per_kw, ref_document, sheet_name, file_name) %>% 
  dplyr::mutate(status_pre_feas = sapply(status_pre_feas, change_ticks),
                
                status_feas = sapply(status_feas, change_ticks),
                status_committed = sapply(status_committed, change_ticks),
                status_candidate = sapply(status_candidate, change_ticks))

df_gfz_projects_database


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_projects_database_gfz_data',
#                       df_gfz_projects_database)



# unique(df_connections_xlsx$csc)
# length(unique(df_connections_xlsx$csc))
# 
# table(df_connections_xlsx$csc, df_connections_xlsx$status)
# table(df_connections_xlsx$csc, df_connections_xlsx$type_of_account)
# table(df_connections_xlsx$type_of_account)
# 
# # df_connections_xlsx %>%
# #   mutate(across(where(is.POSIXt), as.character),
# #          )
# 
# 
# 
# # # 
# # # library(lubridate)
# # # 
# # # data <- data.frame(x1 = 1:3,    # Create example data frame
# # #                    x2 = as.Date(c("2022-10-10", "2024-03-19", "2022-12-07")),
# # #                    x3 = c("2023-11-02", "2025-01-17", "2022-10-04"))
# # # data                            # Print example data frame
# # # sapply(data, is.Date) 
# # # 
# # # 
# # # sapply(df_connections_xlsx, is.POSIXt)
# # 
# # 
# # 
# # RSQLite::dbWriteTable(dbConnect(SQLite(),
# #                                 paste0(write_to_combined_db,
# #                                        "/",
# #                                        combined_sqlite)),
# #                       name =  'connections',
# #                       df_connections_xlsx %>% 
# #                         dplyr::mutate(across(where(is.POSIXt),
# #                                              as.character)))
# 
# 
# max(df_connections_xlsx$status_date)
# min(df_connections_xlsx$status_date)
# 
# 
# max(df_connections_xlsx$application_date)
# min(df_connections_xlsx$application_date)
# 
# max(df_connections_xlsx$quotation_prepared_date)
# min(df_connections_xlsx$quotation_prepared_date)

### This is the end of this script ####


