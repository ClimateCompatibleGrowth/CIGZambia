### This script is for extracting LCMS data (relevant for Gender and Social Inclusion analysis) ###

#Load packages
library(tidyverse)
library(RSQLite)


## Point the software where to look (you will have to specify on your computer)
write_to_data_lake2_db <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data" # provide the path where the raw data is stored
combined_sqlite <- "combined_data.sqlite" # this is the name of the files (datalake database)
data_lake2_sqlite <- "data_lake2.sqlite" # this is the name of the files (datalake database) -- another naming

lcms_2015_folder <- "/home/tembo/Documents/modelling/lcms_project/data" # path to LCMS databases 
person_db <- "lcms2015_sectionsPerson.db" # LCMS household member database
hhold_db <- "lcms2015_sectionHousehold.db" # LCMS household database


#### ---- Processing the data ---- ####

db_conn_pers <- DBI::dbConnect(RSQLite::SQLite(),
                          dbname = paste0(lcms_2015_folder,
                                          "/", person_db))
db_conn_pers

db_comb_tbls_pers <- DBI::dbListTables(db_conn_pers)
# db_comb_tbls_pers

db_conn_hh <- DBI::dbConnect(RSQLite::SQLite(),
                          dbname = paste0(lcms_2015_folder,
                                          "/", hhold_db))
db_conn_hh

db_comb_tbls_hh <- DBI::dbListTables(db_conn_hh)
# db_comb_tbls_hh


## -- Collating data ---- ##

### General
df_general <- dplyr::tbl(db_conn_pers, 'general') %>% 
  dplyr::select(parentid1, pkey_memberid, province = prov, region) %>% 
  dplyr::as_tibble() 
  # colnames()

df_general

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_data_lake2_db,
#                                        "/",
#                                        data_lake2_sqlite)),
#                       name = 'lcms2015_general_data',
#                       df_general)


### Section 1
df_sec1 <- dplyr::tbl(db_conn_pers, 'section1') %>% 
  dplyr::mutate(sec1q7_adj = as.numeric(sec1q7) + as.numeric(sec1q7.),
                sec1q7_adj = ifelse(sec1q7_adj == 0,
                                    NA, sec1q7_adj)) %>% 
  dplyr::select(parentid1, pkey_memberid, sec1q3a, sec1q3b, 
                sec1q4, sec1q5, sec1q6, sec1q7 = sec1q7_adj) %>%
  dplyr::as_tibble()
# colnames()

df_sec1

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_data_lake2_db,
#                                        "/",
#                                        data_lake2_sqlite)),
#                       name = 'lcms2015_section1_data',
#                       df_sec1)


### Section 2
df_sec2 <- dplyr::tbl(db_conn_pers, 'section2') %>%
  dplyr::as_tibble()
# colnames()

df_sec2

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_data_lake2_db,
#                                        "/",
#                                        data_lake2_sqlite)),
#                       name = 'lcms2015_section2_data',
#                       df_sec2)


### Section 4
df_sec4 <- dplyr::tbl(db_conn_pers, 'section4') %>% 
  dplyr::select(parentid1, pkey_memberid, sec4q1, sec4q2, sec4q5, sec4q8) %>%
  dplyr::as_tibble()
# colnames()

df_sec4

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_data_lake2_db,
#                                        "/",
#                                        data_lake2_sqlite)),
#                       name = 'lcms2015_section4_data',
#                       df_sec4)


### Section 5
df_sec5 <- dplyr::tbl(db_conn_pers, 'section5') %>%
  dplyr::select(parentid1, pkey_memberid, sec5q1, sec5q2a, sec5q2code,
                sec5q3a, sec5q3code, sec5q4, sec5q5, sec5q6, sec5q7,
                sec5q8, sec5q9, sec5q10, sec5q11, sec5q11code, sec5q12,
                sec5q12code, sec5q13, sec5q14, sec5q15, sec5q16,
                sec5q17, sec5q18, sec5q19, sec5q20) %>%
  dplyr::as_tibble()
  # colnames()

df_sec5

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_data_lake2_db,
#                                        "/",
#                                        data_lake2_sqlite)),
#                       name = 'lcms2015_section5_data',
#                       df_sec5)



### CSO 2015 Poverty Analysis
df_pa <- dplyr::tbl(db_conn_hh, 'CSO 2015 Poverty Analysis') %>% 
  # dplyr::mutate(sec1q7_adj = as.numeric(sec1q7) + as.numeric(sec1q7.),
  #               sec1q7_adj = ifelse(sec1q7_adj == 0,
  #                                   NA, sec1q7_adj)) %>% 
  dplyr::select(parentid1, region, province = prov, stratum, cluster, intday,
                intmonth, intyear, hhweight, hhsize, popweight, adulteq,
                poverty = Poverty) %>%
  dplyr::as_tibble()
# colnames()

df_pa

# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_data_lake2_db,
#                                        "/",
#                                        data_lake2_sqlite)),
#                       name = 'lcms2015_poverty_analysis_data',
#                       df_pa)



# ##
# db_conn_cig <- DBI::dbConnect(RSQLite::SQLite(),
#                               dbname = paste0(write_to_data_lake2_db,
#                                               "/", data_lake2_sqlite))
# db_conn_cig
# 
# db_comb_tbls_cig <- DBI::dbListTables(db_conn_cig)
# db_comb_tbls_cig

  
### This is the end of this script ####


