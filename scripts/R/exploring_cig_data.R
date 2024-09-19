### This script list all the data available in the project folder (E014.3_IRP) ###

#Load packages
library(tidyverse)


## Point the software where to look (you will have to specify on your computer)
cig_folder <- "~/Documents/Consulting Work/CIG/work/E014.3_IRP" # provide the path where the raw data can stored


## list all folders in the project folder
lst_dirs <- list.dirs(path = cig_folder)

## list all files in the folders
for (dir in lst_dirs){
  
  lst_files <- list.files(path = dir)
  
  print(length(lst_files))
  print(dir)
  print(lst_files)
  
}


### This is the end of this script ####
