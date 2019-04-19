# USoB Documentation

## general notes

  * `$ gcloud config list` should be something like the following:
  ```
  $ gcloud config list
  [compute]
  zone = us-east1-d
  [core]
  account = <your gmail account>
  disable_usage_reporting = True
  project = <your Google Cloud Project name>
  ```
  * Note that `account` is set to an account that has this role on your GS bucket: Storage Object Admin
    * TO DO: figure out the right permissions for a service account only deployment

## Directions for how to run the pipeline for different geometries

The pipeline needs to run once for each geometry. 

1. Run the code in `pre_dataflow_dataset_import.R`
This R script obtains geographic data from the US Census using the `tidycensus` package, formats the data as JSON, and outputs that JSON to a local file. Currently this script does this with three geographies:  
  * **US census tracts** - dataflow_us_census_tract_shapes.json
  * **State Congressional Districts** - dataflow_us_state_house_shapes.json
  * **State Senate Districts** - dataflow_us_state_senate_shapes.json
  * Use these JSON files to create BigQuery tables
    * current, manual method: upload json files to a GCS bucket
    * create a new table with each json file in the BigQuery web UI. you should specify the schema manually when importing using the schema: 
      * `district`:STRING:NULLABLE
      * `Values`:STRING:REPEATED
      * `state`:STRING:NULLABLE
2. Run the following:
  * dataflow_spatial_join_census_tracts.py
  * dataflow_spatial_join_state_house.py
  * dataflow_spatial_join_state_senate.py

Once you've got that output, the SQL queries file takes the output of dataflow and gets it setup for the .R files: post_dataflow_dataset_import.R and mapbox_pipeline_wrapper.R

## BigQuery Tables

There are 22 tables. 

* 6 of them are FCC 477 tables, 
* 16 of them are NDT related. 
  * Of the 16 NDT tables, one is a test location table called `US_locs`. 
  * The other 15 are broken up into 3 sets of 5 tables. 
    * Each set corresponds to one of the three geometry types and is obtained by running the same process with different table names. 

  * **US_loc**: this is the unique locations and states from the NDT table gotten through a `SELECT DISTINCT lat, long, state` call on the NDT table
  * **Polygons_census_tract**: this is to_dataflow_string for census tracts
  * **dataflow_output_census_tract**: output of the `dataflow_spatial_join.py` file for census tracts
  * **ndt_spatial_census_tract**: output of #1 from the SQL queries file for census tracts
  * **Aggregated_MLab_DL_census**: output of #2 from the SQL queries file using DL for census tracts
  * **Aggregated_MLab_UL_census**: output of #2 from the SQL queries file using UL for census tracts
  * **Polygons_state_house**: this is `to_dataflow_string` for `state_house`
  * **dataflow_output_state_house**: output of the `dataflow_spatial_join.py` file for state house
  * **ndt_spatial_state_house**: output of #1 from the SQL queries file for state house
  * **Aggregated_MLab_DL_state_house**: output of SQL query #2 using DL for state house
  * **Aggregated_MLab_UL_state_house**: output of SQL query #2 using UL for state house
  * **Polygons_state_senate**: this is `to_dataflow_string` for state senate
  * **dataflow_output_state_senate**: output of the `dataflow_spatial_join.py` file for state senate
  * **ndt_spatial_state_senate**: output of SQL query #1 for state senate
  * **Aggregated_MLab_DL_state_senate**: output of SQL query #2 using DL for state senate
  * **Aggregated_MLab_UL_state_senate**: output of SQL query #2 using UL for state senate

## FCC 477 tables

I'm a little hazier on how I built the FCC tables, but if I remember correctly, the tables were too big to join locally, so what I did was:

* download the table from the FCC's page (which takes a while), 
* upload the table from a single time period to a google bucket, 
* import that to BQ 
  * and then use SQL query #3 for each table. 

## Maps

For the maps, I added a new section onto `mapbox_pipeline_wrapper.R`, which I should have split out on its own but I was rushing and I wanted to have immediate access to the rest of the code for troubleshooting. I'm attaching it inside mapbox_pipeline_wrapper.R because that troubleshooting might still be useful. It's separated by a big ##THIS IS NEW##. 

## End to End quick instructions

Quick Summary

* Query for M-Lab NDT data
* Format and join NDT data with census tract data using Dataflow
* Compute aggregate data by geography using R code

## Add a new FCC release

