### This script is for collating generation data from the project folder (E014.3_IRP) to a datalake database ###
## You will not that generation has as series of datalakes database (before the generation data is pushed to the main datalake database)
## because the files under the generation folder and more messy data. 
## Therefore, the processing and further processing of generation data was significantly more

## Load packages
library(tidyverse)
library(zoo)
library(RSQLite)

## Point the software where to look (you will have to specify on your computer)
cig_folder <- "~/Documents/Consulting Work/CIG/work/" # # provide the path where the raw data is stored
generation_folder <- "E014.3_IRP/2. Implementation/4. Workstreams/Generation Planning_copy" #this is the folder that has the raw generation data
## Note, the files with transmission substation data were converted from .xlsb format to .xlsx format. 
# This was done by opening them and then saving them as .xlsx format. It was necessary to make the reading in R easier.

write_to_generation_db <- "/media/tembo/Seagate Portable Drive/Consulting Work/CIG/work/cigzambia_output/data/generation_data"
write_to_combined_db <- "/home/tembo/Documents/Consulting Work/CIG/work/cigzambia_output/data/combined_data"
combined_sqlite <- "combined_data.sqlite"
generat_sqlite <- "generation_data.sqlite"
generat_2_sqlite <- "generation_data_2.sqlite"
generat_3_sqlite <- "generation_data_3.sqlite"

gen_folder <- paste0(cig_folder, generation_folder)


# List all folder in the generations folder
dirs_list <- list.dirs(path = gen_folder)
dirs_list


# Function for collating data from the main folder
collate_data_by_file_extension <- function(FolderPathInput, InputFileExtList){
  
  # dataVal_staff <- NULL
  
  dirs_list <- list.dirs(path = FolderPathInput)
  
  df_files <- NULL
  
  series_num <- 0
  
  file_ext_lst <- InputFileExtList
  
  for (file_ext in file_ext_lst){
    
    for (dir in dirs_list){
      print(dir) ## Directory name
      
      files <- list.files(path = dir,
                          pattern = file_ext)
      # print(files)
      
      for (file in files){
        print(file)  ## File name
        
        file_path <- paste0(dir, "/", file)
        print(file_path)
        
        series_num = series_num + 1
        
        print(series_num)
        
        df_file <- dplyr::bind_cols(series = series_num,
                                    file_path = file_path,
                                    file_ext = file_ext)
        
        
        
        df_files <- dplyr::bind_rows(df_files,
                                     df_file)
        
        
      }
      
    }
    
  }
  
  return(df_files %>% 
           dplyr::distinct())
  
}

file_ext_lst <- c('.xlsx', '.csv', '.xls', '.xlsm', '.txt', '.xlsb')

df_cig_gen <- collate_data_by_file_extension(gen_folder, file_ext_lst)
df_cig_gen


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_generation_db,
#                                        "/",
#                                        generat_sqlite)),
#                       name =  'generation_file_path_data',
#                       df_cig_gen)
# 
# gc()





# Function for collating data from the main folder
organise_data_by_file_extension_from_main_folder <- function(InputData, InputFileExtList){
  
  dataVal <- InputData %>% 
    dplyr::select(-series, -file_ext) %>% 
    dplyr::distinct()
  
  print(dim(dataVal))
  
  file_ext_lst <- InputFileExtList
  
  df_files <- NULL
  
  for (file_ext in file_ext_lst){
    
    print(file_ext)
    
    lst_Val <- as.character(dataVal$file_path)
    
    test_ext <- function(InputList){
      stringr::str_ends(InputList, file_ext)
    }
    
    test_ext_lst <- lapply(lst_Val, test_ext)
    
    df_file <- dataVal %>%
      dplyr::mutate(ext = file_ext) %>%
      dplyr::bind_cols(test_ext = unlist(test_ext_lst)) %>%
      dplyr::filter(test_ext)  %>%
      dplyr::select(-test_ext) %>%  #-series, -test_ext, -file_ext
      dplyr::distinct()
    
    print(dim(df_file))
    
    df_files <- dplyr::bind_rows(df_files,
                                 df_file)
    
  }
  
  return(df_files %>%
           dplyr::arrange(file_path))
  
}

df_cig_gen_folder_organised <- organise_data_by_file_extension_from_main_folder(df_cig_gen, file_ext_lst)
df_cig_gen_folder_organised

# table(df_cig_gen$file_ext)
# table(df_cig_gen_folder_organised$ext)



# Function for collating generation data from the folder and immediately writing it to a datalake
process_generation_data_wide <- function(InputData, InputFileExt){
  
  dataVal <- InputData
  
  file_ext_lst <- unique(dataVal$ext)
  
  # data_all <- NULL
  data_all_dim <- NULL
  
  if(InputFileExt %in% c('.xlsx')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      print(file_name)
      
      data_all <- NULL
      
      sheet_names <- readxl::excel_sheets(path = file_name)
      
      for (sheet_name in sheet_names){
        
        file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
        file_name_cleaned <- dplyr::last(file_name_cleaned)
        
        file_name_cleaned_2 <- stringr::str_replace_all(file_name_cleaned, " ", "_")
        
        file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
        file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
        
        sheet_names <- readxl::excel_sheets(path = file_name)
        
        for (sheet_name in sheet_names){
          
          dataVal2 <- readxl::read_excel(file_name,
                                         sheet = sheet_name)
          
          num_cols <- dim(dataVal2)[2]
          
          if(num_cols > 0){

            dataVal3 <- dataVal2 %>%
              dplyr::mutate(across(everything(), as.character),
                            sheet_name = sheet_name,
                            file_name = file_name_cleaned,
                            file_ext = InputFileExt,
                            folder_path = file_val)
            
            df_dim <- t(as.data.frame(dim(dataVal3))) %>% 
              dplyr::as_tibble() %>% 
              dplyr::mutate(sheet_name = sheet_name,
                            file_name = file_name_cleaned,
                            file_ext = InputFileExt) %>% 
              dplyr::rename(num_rows = V1,
                            num_cols = V2)
            # print(df_dim)
            print(dim(dataVal3))

            data_all_dim <- dplyr::bind_rows(data_all_dim, df_dim)
            # data_all <- dplyr::bind_rows(data_all, dataVal3)
            
            RSQLite::dbWriteTable(dbConnect(SQLite(),
                                            paste0(write_to_generation_db,
                                                   "/",
                                                   generat_2_sqlite)),
                                  name =  paste0('gen_raw_', 
                                                 file_name_cleaned_2,
                                                 '_', sheet_name),
                                  dataVal3,
                                  append = TRUE)

          }
          
        }
        
      }
      
      # RSQLite::dbWriteTable(dbConnect(SQLite(),
      #                                 paste0(write_to_generation_db,
      #                                        "/",
      #                                        generat_2_sqlite)),
      #                       name =  paste0('gen_raw_', file_name_cleaned_2),
      #                       data_all,
      #                       append = TRUE)
      # 
    }
    
  }
  
  if(InputFileExt %in% c('.xls')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      print(file_name)
      
      data_all <- NULL
      
      sheet_names <- readxl::excel_sheets(path = file_name)
      
      for (sheet_name in sheet_names){
        
        file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
        file_name_cleaned <- dplyr::last(file_name_cleaned)
        
        file_name_cleaned_2 <- stringr::str_replace_all(file_name_cleaned, " ", "_")
        
        file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
        file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
        
        sheet_names <- readxl::excel_sheets(path = file_name)
        
        for (sheet_name in sheet_names){
          
          dataVal2 <- readxl::read_excel(file_name,
                                         sheet = sheet_name)
          
          num_cols <- dim(dataVal2)[2]
          
          if(num_cols > 0){

            dataVal3 <- dataVal2 %>%
              dplyr::mutate(across(everything(), as.character),
                            sheet_name = sheet_name,
                            file_name = file_name_cleaned,
                            file_ext = InputFileExt,
                            folder_path = file_val)
            
            df_dim <- t(as.data.frame(dim(dataVal3))) %>% 
              dplyr::as_tibble() %>% 
              dplyr::mutate(sheet_name = sheet_name,
                            file_name = file_name_cleaned,
                            file_ext = InputFileExt) %>% 
              dplyr::rename(num_rows = V1,
                            num_cols = V2)
            # print(df_dim)
            print(dim(dataVal3))
            
            data_all_dim <- dplyr::bind_rows(data_all_dim, df_dim)
            # data_all <- dplyr::bind_rows(data_all, dataVal3)
            
            RSQLite::dbWriteTable(dbConnect(SQLite(),
                                            paste0(write_to_generation_db,
                                                   "/",
                                                   generat_2_sqlite)),
                                  name =  paste0('gen_raw_', 
                                                 file_name_cleaned_2,
                                                 '_', sheet_name),
                                  dataVal3,
                                  append = TRUE)

          }
          
        }
        
      }
      
      # RSQLite::dbWriteTable(dbConnect(SQLite(),
      #                                 paste0(write_to_generation_db,
      #                                        "/",
      #                                        generat_2_sqlite)),
      #                       name =  paste0('gen_raw_', file_name_cleaned_2),
      #                       data_all,
      #                       append = TRUE)
      
    }
    
  }
  
  if(InputFileExt %in% c('.csv')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
      file_name_cleaned <- dplyr::last(file_name_cleaned)
      
      file_name_cleaned_2 <- stringr::str_replace_all(file_name_cleaned, " ", "_")
      
      file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
      file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
      
      dataVal2 <- readr::read_csv(file_name,
                                  col_names = FALSE,
                                  show_col_types = FALSE)
      
      num_cols <- dim(dataVal2)[2]
       
      if(num_cols > 0){

        dataVal3 <- dataVal2 %>%
          dplyr::mutate(across(everything(), as.character),
                        file_name = file_name_cleaned,
                        file_ext = InputFileExt,
                        folder_path = file_val)
        
        df_dim <- t(as.data.frame(dim(dataVal3))) %>% 
          dplyr::as_tibble() %>% 
          dplyr::mutate(file_name = file_name_cleaned,
                        file_ext = InputFileExt) %>% 
          dplyr::rename(num_rows = V1,
                        num_cols = V2)
        # print(df_dim)
        print(dim(dataVal3))
        
        data_all_dim <- dplyr::bind_rows(data_all_dim, df_dim)

        RSQLite::dbWriteTable(dbConnect(SQLite(),
                                        paste0(write_to_generation_db,
                                               "/",
                                               generat_2_sqlite)),
                              name =  paste0('gen_raw_', file_name_cleaned_2),
                              dataVal3,
                              append = TRUE)

      }
      
    }
    
  }
  
  if(InputFileExt %in% c('.txt')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
      file_name_cleaned <- dplyr::last(file_name_cleaned)
      
      file_name_cleaned_2 <- stringr::str_replace_all(file_name_cleaned, " ", "_")
      
      file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
      file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
      
      dataVal2 <- readr::read_delim(file_name,
                                    col_names = FALSE,
                                    delim = "\t",
                                    show_col_types = FALSE)
      
      num_cols <- dim(dataVal2)[2]
      
      if(num_cols > 0){

        dataVal3 <- dataVal2 %>%
          dplyr::mutate(across(everything(), as.character),
                        file_name = file_name_cleaned,
                        file_ext = InputFileExt,
                        folder_path = file_val)
        
        df_dim <- t(as.data.frame(dim(dataVal3))) %>% 
          dplyr::as_tibble() %>% 
          dplyr::mutate(file_name = file_name_cleaned,
                        file_ext = InputFileExt) %>% 
          dplyr::rename(num_rows = V1,
                        num_cols = V2)
        # print(df_dim)
        print(dim(dataVal3))
        
        data_all_dim <- dplyr::bind_rows(data_all_dim, df_dim)

        RSQLite::dbWriteTable(dbConnect(SQLite(),
                                        paste0(write_to_generation_db,
                                               "/",
                                               generat_2_sqlite)),
                              name =  paste0('gen_raw_', file_name_cleaned_2),
                              dataVal3,
                              append = TRUE)

      }
      
    }
    
  }
  
  return(data_all_dim)
  
}

gc()


start_time <- Sys.time()

df_gene_data_wide_csv <- process_generation_data_wide(df_cig_gen_folder_organised, '.csv')
Sys.sleep(5)
gc()


df_gene_data_wide_txt <- process_generation_data_wide(df_cig_gen_folder_organised, '.txt')
Sys.sleep(10)
gc()


df_gene_data_wide_xls <- process_generation_data_wide(df_cig_gen_folder_organised, '.xls')
Sys.sleep(30)
gc()

 
df_gene_data_wide_xlsx <- process_generation_data_wide(df_cig_gen_folder_organised, '.xlsx')
Sys.sleep(30)
gc()



end_time <- Sys.time()
time_needed <- end_time - start_time
print(time_needed)


Sys.sleep(20)
gc()


df_gen_dim_merged <- dplyr::bind_rows(df_gene_data_wide_csv,
                                  df_gene_data_wide_txt,
                                  df_gene_data_wide_xlsx,
                                  df_gene_data_wide_xls)

df_gen_dim_merged


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_combined_db,
#                                        "/",
#                                        combined_sqlite)),
#                       name =  'generation_raw_data_files',
#                       df_gen_dim_merged)


# RSQLite::dbWriteTable(dbConnect(SQLite(),
#                                 paste0(write_to_generation_db,
#                                        "/",
#                                        generat_2_sqlite)),
#                       name =  'generation_raw_data_files',
#                       df_gen_dim_merged)



df_gen_dim_summary <- df_gen_dim_merged %>% 
  dplyr::distinct() %>% 
  dplyr::select(-sheet_name) %>% 
  dplyr::group_by(file_name, file_ext) %>% 
  dplyr::summarise(num_rows = sum(num_rows, na.rm = TRUE),
                   num_cols = sum(num_cols, na.rm = TRUE),
                   .groups = 'drop') %>% 
  dplyr::arrange(file_name) #num_cols, num_rows
  

df_gen_dim_summary

# print(dim(df_gene_data_wide_xlsx))
# print(dim(df_gene_data_wide_xlsx %>% 
#             dplyr::distinct()))
# 
# print(dim(df_gene_data_wide_xls))
# print(dim(df_gene_data_wide_xls %>% 
#             dplyr::distinct()))




# Function for processing collated generation data from a datalake
merging_same_file_generation_data <- function(InputFileExt){
  
  db_conn_gen <- DBI::dbConnect(RSQLite::SQLite(),
                            dbname = paste0(write_to_generation_db,
                                            "/", generat_2_sqlite))
  
  db_tbls_lst <- sort(DBI::dbListTables(db_conn_gen))
  
  if(InputFileExt %in% c('.xlsx')){
    
    df_xlsx <- dplyr::bind_cols(db_table = db_tbls_lst) %>% 
      dplyr::filter(stringr::str_detect(db_table, '.xlsx'))
    
    print(df_xlsx)
    
    df_split <- stringr::str_split_fixed(as.character(df_xlsx$db_table),
                                         pattern = '.xlsx',
                                   2) %>% 
      dplyr::as_tibble()
    
    file_name_lst <- setdiff(unique(df_split$V1), 
                             "gen_raw_Excel_Template-IRP_input_Data_(1)")
    file_name_lst <- setdiff(file_name_lst,
                             "gen_raw_Generation-Projects-Database-GFZ-27042020_(1)")
    
    print(length(file_name_lst))
    print(length(unique(file_name_lst)))
    
    for (file_name in file_name_lst){
      
      # print(file_name)
      
      df_tbls <- df_xlsx %>%
        dplyr::filter(stringr::str_detect(db_table, file_name))
      
      print(df_tbls)
      
      file_tbls <- df_tbls$db_table
      
      df_data_all <- NULL
      
      for (file_tbl in file_tbls){
        
        print(file_tbl)
        
        df_gen_tbl_data <- dplyr::tbl(db_conn_gen, file_tbl)
        
        df_data_all <- dplyr::bind_rows(df_data_all, 
                                        df_gen_tbl_data %>%
                                          dplyr::rename_with(tolower) %>% 
                                          dplyr::as_tibble())
        
      }
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(write_to_generation_db,
                                             "/",
                                             generat_3_sqlite)),
                            name =  paste0(file_name, '.xlsx'),
                            df_data_all,
                            append = TRUE)
      
    }


  }
  
  if(InputFileExt %in% c('.xls')){
    
    df_xls <- dplyr::bind_cols(db_table = db_tbls_lst) %>% 
      dplyr::filter(stringr::str_detect(db_table, '.xls_'))
    
    # print(df_xls)
    
    df_split <- stringr::str_split_fixed(as.character(df_xls$db_table),
                                         pattern = '.xls',
                                         2) %>% 
      dplyr::as_tibble()
    
    file_name_lst <- unique(df_split$V1)
    
    print(length(file_name_lst))
    print(length(unique(file_name_lst)))
    
    for (file_name in file_name_lst){
      
      print(file_name)
      
      df_tbls <- df_xls %>% # dplyr::bind_cols(tables = db_tbls_lst) %>%
        dplyr::filter(stringr::str_detect(db_table, file_name))
      
      print(df_tbls)
      
      file_tbls <- df_tbls$db_table
      
      df_data_all <- NULL
      
      for (file_tbl in file_tbls){
        
        print(file_tbl)
        
        df_gen_tbl_data <- dplyr::tbl(db_conn_gen, file_tbl) #%>%
        # dplyr::as_tibble()
        
        df_data_all <- dplyr::bind_rows(df_data_all,
                                        df_gen_tbl_data %>% # head(df_gen_tbl_data, 2)
                                          dplyr::rename_with(tolower) %>% 
                                          dplyr::as_tibble())
        
      }
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(write_to_generation_db,
                                             "/",
                                             generat_3_sqlite)),
                            name =  paste0(file_name, '.xls'),
                            df_data_all,
                            append = TRUE)
      
    }
    
  }
  
  
  if(InputFileExt %in% c('.csv')){
    
    df_csv <- dplyr::bind_cols(db_table = db_tbls_lst) %>% 
      dplyr::filter(stringr::str_detect(db_table, '.csv'))
    
    file_name_lst <- unique(df_csv$db_table)
    
    for (file_name in file_name_lst){
      
      print(file_name)
      
      df_gen_tbl_data <- dplyr::tbl(db_conn_gen, file_name)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(write_to_generation_db,
                                             "/",
                                             generat_3_sqlite)),
                            name = file_name,
                            df_gen_tbl_data %>% 
                              dplyr::rename_with(tolower) %>%
                              dplyr::as_tibble(),
                            append = TRUE)
      
    }
    
  }
  
  
  
  if(InputFileExt %in% c('.txt')){
    
    df_csv <- dplyr::bind_cols(db_table = db_tbls_lst) %>% 
      dplyr::filter(stringr::str_detect(db_table, '.txt'))
    
    file_name_lst <- unique(df_csv$db_table)
    
    for (file_name in file_name_lst){
      
      print(file_name)
      
      df_gen_tbl_data <- dplyr::tbl(db_conn_gen, file_name)
      
      RSQLite::dbWriteTable(dbConnect(SQLite(),
                                      paste0(write_to_generation_db,
                                             "/",
                                             generat_3_sqlite)),
                            name = file_name,
                            df_gen_tbl_data %>% 
                              dplyr::rename_with(tolower) %>%
                              dplyr::as_tibble(),
                            append = TRUE)
      
    }
    
  }
  
  
}

gc()

# start_time <- Sys.time()
# 
# merging_same_file_generation_data('.xlsx')
# 
# merging_same_file_generation_data('.xls')
# 
# merging_same_file_generation_data('.csv')
# 
# merging_same_file_generation_data('.txt')
# 
# end_time <- Sys.time()
# time_needed <- end_time - start_time
# print(time_needed)



# Function for further processing of collated generation data from datalake
process_generation_data <- function(InputData, InputFileExt){
  
  dataVal <- InputData
  
  file_ext_lst <- unique(dataVal$ext)
  
  data_all <- NULL
  
  
  if(InputFileExt %in% c('.xlsx')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      sheet_names <- readxl::excel_sheets(path = file_name)
      
      for (sheet_name in sheet_names){
        
        file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
        file_name_cleaned <- dplyr::last(file_name_cleaned)
        # print(file_name_cleaned)
        
        file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
        file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
        
        sheet_names <- readxl::excel_sheets(path = file_name)
        
        for (sheet_name in sheet_names){
          
          dataVal2 <- readxl::read_excel(file_name,
                                         sheet = sheet_name) %>%
            dplyr::mutate(across(everything(), as.character),
                          sheet_name = sheet_name,
                          file_name = file_name_cleaned, 
                          file_ext = InputFileExt,
                          folder_path = file_val)
          
          
          num_cols <- dim(dataVal2)[2]
          
          # print(dim(dataVal2))
          # print(num_cols)
          # 
          if(num_cols > 3){
            
            dataVal3 <- dataVal2 %>%
              tidyr::pivot_longer(cols = c(-file_ext, -sheet_name, -file_name, -folder_path),
                                  names_to = "variable",
                                  values_to = "value")
            
            # print(dataVal2)
            
            RSQLite::dbWriteTable(dbConnect(SQLite(),
                                            paste0(write_to_generation_db,
                                                   "/",
                                                   generat_sqlite)),
                                  name =  'generation_raw_xlsx_data',
                                  dataVal3,
                                  append = TRUE)
            
            }
          
        }
        
      }
      
    }
    
  }
  
  if(InputFileExt %in% c('.xls')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      sheet_names <- readxl::excel_sheets(path = file_name)
      
      for (sheet_name in sheet_names){
        
        file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
        file_name_cleaned <- dplyr::last(file_name_cleaned)
        # print(file_name_cleaned)
        
        file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
        file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
        
        sheet_names <- readxl::excel_sheets(path = file_name)
        
        for (sheet_name in sheet_names){
          
          dataVal2 <- readxl::read_excel(file_name,
                                         sheet = sheet_name) %>%
            dplyr::mutate(across(everything(), as.character),
                          sheet_name = sheet_name,
                          file_name = file_name_cleaned, 
                          file_ext = InputFileExt,
                          folder_path = file_val)
          
          
          num_cols <- dim(dataVal2)[2]
          
          # print(dim(dataVal2))
          # print(num_cols)
          # 
          if(num_cols > 3){
            
            dataVal3 <- dataVal2 %>%
              tidyr::pivot_longer(cols = c(-file_ext, -sheet_name, -file_name, -folder_path),
                                  names_to = "variable",
                                  values_to = "value")
            
            RSQLite::dbWriteTable(dbConnect(SQLite(),
                                            paste0(write_to_generation_db,
                                                   "/",
                                                   generat_sqlite)),
                                  name =  'generation_raw_xls_data',
                                  dataVal3,
                                  append = TRUE)
            
          }
          
        }
        
      }
      
    }
    
  }
  
  if(InputFileExt %in% c('.csv')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
      file_name_cleaned <- dplyr::last(file_name_cleaned)
      
      file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
      file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
      
      dataVal2 <- readr::read_csv(file_name,
                                  col_names = FALSE,
                                  show_col_types = FALSE) %>%
        dplyr::mutate(across(everything(), as.character),
                      file_name = file_name_cleaned, 
                      file_ext = InputFileExt,
                      folder_path = file_val) 
      
      num_cols <- dim(dataVal2)[2]
      
      # print(dim(dataVal2))
      # print(num_cols)
      # 
      if(num_cols > 3){
        
        dataVal3 <- dataVal2 %>%
          tidyr::pivot_longer(cols = c(-file_ext, -file_name, -folder_path),
                              names_to = "variable",
                              values_to = "value")

        # print(dataVal2)

        RSQLite::dbWriteTable(dbConnect(SQLite(),
                                        paste0(write_to_generation_db,
                                               "/",
                                               generat_sqlite)),
                              name =  'generation_raw_csv_data',
                              dataVal3,
                              append = TRUE)
        
      }
      
    }
    
  }
  
  if(InputFileExt %in% c('.txt')){
    
    df_data <- dataVal %>%
      dplyr::filter(ext == InputFileExt)
    
    file_names <- df_data$file_path
    
    for (file_name in file_names){
      
      file_name_cleaned <- unlist(stringr::str_split(file_name, "/"))
      file_name_cleaned <- dplyr::last(file_name_cleaned)
      
      file_val <- stringr::str_replace(file_name, "/home/tembo/Documents/Consulting Work/CIG/work/", '')
      file_val <- stringr::str_replace(file_val, paste0('/', file_name_cleaned), '')
      
      dataVal2 <- readr::read_delim(file_name,
                                    col_names = FALSE,
                                    delim = "\t",
                                    show_col_types = FALSE) %>%
        dplyr::mutate(across(everything(), as.character),
                      file_name = file_name_cleaned, 
                      file_ext = InputFileExt,
                      folder_path = file_val)
      
      num_cols <- dim(dataVal2)[2]
      
      # print(dim(dataVal2))
      # print(num_cols)
      # 
      if(num_cols > 3){
        
        dataVal3 <- dataVal2 %>%
          tidyr::pivot_longer(cols = c(-file_ext, -file_name, -folder_path),
                              names_to = "variable",
                              values_to = "value")
        
        RSQLite::dbWriteTable(dbConnect(SQLite(),
                                        paste0(write_to_generation_db,
                                               "/",
                                               generat_sqlite)),
                              name =  'generation_raw_txt_data',
                              dataVal3,
                              append = TRUE)
        
      }
      
    }
    
  }
  
  return(data_all)
  
}

gc()



start_time <- Sys.time()
## c('.csv', '.xlsx', '.xlsm') # '.txt', '.xlsx', 

df_gene_data_xlsx <- process_generation_data(df_cig_gen_folder, '.xlsx', gen_folder)
df_gene_data_xls <- process_generation_data(df_cig_gen_folder, '.xls', gen_folder)
df_gene_data_csv <- process_generation_data(df_cig_gen_folder, '.csv', gen_folder)
df_gene_data_xlsm <- process_generation_data(df_cig_gen_folder, '.xlsm', gen_folder)
df_gene_data_txt <- process_generation_data(df_cig_gen_folder, '.txt', gen_folder)

end_time <- Sys.time()
time_needed <- end_time - start_time
print(time_needed)


Sys.sleep(10)
gc()


### This is the end of this script ####


