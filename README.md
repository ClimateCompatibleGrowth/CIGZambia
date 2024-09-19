# CIGZambia


## Overview
Collection of resources for the **Cities and Infrastructure for Growth Zambia (CIGZambia)** project.

## Folder Structure

- `/docs` - Contains markdown documentation for each R script explaining the data collation and processing workflows.
- `/scripts/R` - Contains R scripts responsible for handling different datasets, including connections, consumption, generation, and more.

## R Scripts and Documentation

Below are the main scripts used in this project along with links to their respective documentation:

### Demand Data Processing

- **[collating_cig_demand_connections_data.R](scripts/R/collating_cig_demand_connections_data.R)**  
  Documented in [collating_cig_demand_connections_data.md](docs/collating_cig_demand_connections_data.md).  
  Processes data related to demand connections for CIG.

- **[collating_cig_demand_consumption_data.R](scripts/R/collating_cig_demand_consumption_data.R)**  
  Documented in [collating_cig_demand_consumption_data.md](docs/collating_cig_demand_consumption_data.md).  
  Focuses on the collation of consumption data for CIG.

- **[collating_cig_demand_energy_sales_data.R](scripts/R/collating_cig_demand_energy_sales_data.R)**  
  Documented in [collating_cig_demand_energy_sales_data.md](docs/collating_cig_demand_energy_sales_data.md).  
  Processes energy sales data.

- **[collating_cig_demand_transmission_data.R](scripts/R/collating_cig_demand_transmission_data.R)**  
  Documented in [collating_cig_demand_transmission_data.md](docs/collating_cig_demand_transmission_data.md).  
  Handles transmission data processing for CIG demand.

### Generation Data Processing

- **[collating_cig_generation_data.R](scripts/R/collating_cig_generation_data.R)**  
  Documented in [collating_cig_generation_data.md](docs/collating_cig_generation_data.md).  
  Focuses on collating generation data for CIG.

- **[processing_cig_generation_data.R](scripts/R/processing_cig_generation_data.R)**  
  Documented in [processing_cig_generation_data.md](docs/processing_cig_generation_data.md).  
  Further processing of generation data to integrate it with the overall CIG dataset.

### Final Processing and Database

- **[collating_further_processing_cig_combined_data.R](scripts/R/collating_further_processing_cig_combined_data.R)**  
  Documented in [collating_further_processing_cig_combined_data.md](docs/collating_further_processing_cig_combined_data.md).  
  Final collation and processing to combine all datasets into a single database.

- **[updating_final_database.R](scripts/R/updating_final_database.R)**  
    Documented in [updating_final_database.md](docs/updating_final_database.md).  
  Updates the final database with the most recent data.


### Additional Scripts and Documentation

- **[exploring_cig_data.R](scripts/R/exploring_cig_data.R)**  
  Script for exploring the combined CIG data for insights and verification.
  
- **[collating_lcms2015_data_for_cig.R](scripts/R/collating_lcms2015_data_for_cig.R)**  
  Documented in [collating_lcms2015_data_for_cig.md](docs/collating_lcms2015_data_for_cig.md).  
  Collates LCMS 2015 data for integration into the CIG database.