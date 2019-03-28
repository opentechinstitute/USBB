# United States of Broadband (USB)
A private repository for the working draft of the United States of Broadband/State of the Internet OTI project

The [maps on the landing page for this project](https://opentechinstitute.github.io/USBB/SOTI.html) use data that come from the M-Lab NDT 
tables in BigQuery. However, to get them in the right format and join them with census tract data, they get processed by Dataflow and 
aggregated in R. The code for that pipeline is in this repository. 

There are three four in here and one function file.

The four scripts are:

1) pre_dataflow_dataset_import, which gets census data and writes it out in the right format for the Python dataflow script,

2) dataflow_spatial_join, which takes the output of the previous script and performs a spatial join with it,

3) post_dataflow_dataset_import, which gets the joined data from BigQuery after the dataflow script has run,

4) mapbox_pipeline_wrapper, which processes the BigQuery data and produces the geojson map layers and json data layer for Mapbox.

To use this pipeline, download the four script files in the repo as well as pipeline functions.R and put them in the same folder. 

Open 1,3, and 4 in R. Run 1 and 2. 

Follow the instructions for running the dataflow pipeline 1.ii

Run 4.

That will produce the geojson and json files that the Mapbox map embedded in the project landing page needs. The pipeline also produces 
several other R objects as auxilliary files. These are data files that are used in mapbox_pipeline_wrapper.

## BQ Tables
The BQ tables that are needed to run the pipeline all live in the thieme dataset inside the mlab-sandbox project. Many of the tables below have two names. This is because I did a bad job of naming them the first time around. The new, more regular names are the ones outside the parentheses. The older names are inside the parentheses. The reason I’m giving both is that BQ doesn’t allow for renaming tables, you have to copy the table to rename it. However, BQ also has a great record system that lets you follow the chain of what table was used to produce another table. Since those record use the original names, I’m keeping both for pipeline auditing purposes. 

- `mlab-sandbox`
  - `thieme`
     - `Polygons_census_tract` (Original: tract_state)
        - Stringified census tract polygons for dataflow
     - `US_loc`
        - table containing the unique locations of the NDT tables as of August 17th, 2018.
     - `dataflow_output_census_tract` (Original: dataflow_DL)
        - table containing the unique locations of the NDT tables and their corresponding census tract as of August 20th 2018
     - `ndt_spatial_census_tract` (Original: ndt_spatial)
        - copy of US NDT download table with corresponding state house district, as of Aug 31st, 2018
     - `Aggregated_MLab_DL_census` (Original: dataflow_county_final_copy)
        - Aggregated M-Lab download and RTT data by census tract as of Dec 13, 2018
     - `Aggregated_MLab_UL_census` (Original: test_UL)
        - Aggregated M-Lab upload data by census tract as of Dec 13, 2018
     - `Polygons_state_house` (Original: dataflow_larger_area_poly_house)
        - Stringified house district polygons for dataflow
     - `dataflow_output_state_house` (Original: dataflow_mapbox_lower_DL_new_1_area_mult)
        - table containing the unique locations of the NDT tables and their corresponding state house district as of Dec 12th 2018
     - `ndt_spatial_state_house` (Original: dataflow_lower_test_int_final)
        - copy of US NDT download table with corresponding state house district, as of Dec 13, 2018
     - `Aggregated_MLab_DL_state_house` (Original: dataflow_lower_test_final)
        - Aggregated M-Lab download and RTT data by state house as of Dec 13, 2018
     - `Aggregated_MLab_UL_state_house` (Original: mixed_house)
        - Aggregated M-Lab upload data by state house as of Dec 20, 2018
     - `Polygons_state_senate` (Original: dataflow_larger_area_poly_senate)
        - Stringified senate district polygons for dataflow
     - `dataflow_output_state_senate` (Original: dataflow_mapbox_upper_DL_new_1_area_mult)
        - table containing the unique locations of the NDT tables and their corresponding state senate district as of Dec 12th 2018
     - `ndt_spatial_state_senate` (Original: dataflow_upper_final_int)
        - copy of US NDT download table with corresponding state house district, as of Dec 20, 2018
     - `Aggregated_MLab_DL_state_senate` (Original: dataflow_upper_final)
        - Aggregated M-Lab download and RTT data by state senate as of Dec 13, 2018
     - `Aggregated_MLab_UL_state_senate` (Original: UL_test_senate_joined)
        - Aggregated M-Lab upload data by state house as of Dec 13, 2018
     - `477_dec_2014`
        - The full FCC 477 table from December 2014
     - `477_jun_2015`
        - The full FCC 477 table from Jun 2015
     - `477_dec_2015`
        - The full FCC 477 table from December 2015
     - `477_jun_2016`
        - The full FCC 477 table from June 2016
     - `477_dec_2016`
        - The full FCC 477 table from December 2016
     - `477_jun_2017`
        - The full FCC 477 table from June 2017

## Adding a new dataset to the Mapbox map. 
1. _If you need to add new geographic region:_
     1. Create a BQ table of stringified polygons like `thieme.Polygons_census_tract`. This table should come from a JSON and should have three variables, the identifier of the geographic regions (tract, district, etc), the stringified polygons themselves (in the form of [[x_1,y_1],[x_2,y_2],…,[x_n,y_n]]), and the region that polygon belongs to (state in the case of the U.S.).
     2. Run dataflow_spatial_join.py in the opentechinstitute/SOTI. Some parameters need to be tweaked:
        1. In code block [1], the ‘district’ in  “tract = element['district']” should be changed to the name of the first variable in the JSON above (the identifier of the geographic regions)
        2. In code block [5], project, staging location, temp_location, and setup_file should be changed to the user’s locations. Likewise, machine-type, disk_size_gb, and num_workers should be set within the user’s financial constraints. 
        3. In code block [6], table and dataset should be set to the name of the table and dataset containing the polygons from step 1 above. 
        4. In code block [7], table and dataset should be set to the desired name for the output table of unique locations joined with geographic regions like `thieme.dataflow_output_census_tract`
     3. Run the first SQL query in opentechinstitute/SOTI/SQL_queries replacing “DATAFLOW_OUTPUT_TABLE” with the name of the BQ table produced by 1.ii. above. This query has to be saved to a table because the output is too large to display directly. This produces tables that are copies of the NDT table with an additional variable corresponding to the census district like `ndt_spatial_census_tract` above. 
     4. Run the second SQL query in opentechinstitute/SOTI/SQL_queries replacing “OUTPUT_OF_ONE” with the name of the BQ table produced by 1.iii. This query has to be saved to a table because the output is too large to display directly. This produces tables containing aggregated statistics to be used in R like `Aggregated_MLab_DL_census` above.
2. _Adding new data to the JSON file output in R:_

The R script “mapbox_pipeline_wrapper.R” outputs a JSON file that Mapbox uses to populate a map. The structure of the JSON is {county, leg:{house, senate}} and this structure is created from line 300 on. There are different kinds of new data you could add to this JSON and you follow slightly different processes for each. 

   1. _To add a new data type to an existing set of geographic regions:_	
       1. Aggregate the data type by FCC data release (June 2015, December 2015,…) and geographic region. Do this in one variable for county and one variable for state house/state senate. 
       2. Similar to the way “D_mlab_census_shape_summed” and “D_joined_summed_tog” are joined on line 85, join the county version of the new variable to “D_data_county” after line 102. 
       3. Join the state house/state senate version of the new variable to “D_data” after line 284.
       4. The JSON resulting from running the pipeline after making these changes will have the new data.
   2. _To add a new geographic type:_
       1. Write a SQL query that imports the new data similar to the ones at the beginning of “post_dataflow_dataset_import.R” 
       2. Load the geographic data that was stringified in step 1.i to make the dataflow input. The data should come as a shapefile. In the existing cases, they are census shapefiles that came from tidycensus in R. 
       3. Once the desired data is in, the goal is to create a data layer and a map layer from this data. To make the data layer, follow lines 67-102 or lines 162-203. 67-102 provide a good example of aggregating FCC and M-Lab data for county shapefiles. 162-203 do the same thing for state house/state senate. Both code chunks do similar things and making the data layer for the new geographies will repeat this code. Create a data frame like on line 284, containing the new data.
       4. The data layer is output as a JSON with a particular structure. Currently, it is {county, leg: {state house, state senate}}. New geographies should be added as {county, leg: {state house, state senate}, new geo}. Lines 303-312 create the nested JSON.
          1. First, turn the data frame into a JSON.
          2. Write the JSON out.
          3. Read the JSON in.
          4. Combine the new geography’s JSON with the existing ones on line 312. 
          5. Write out the nested JSON.
       5. The map layer is made by taking the shapefiles and exporting them as a GEOJSON using the function st_write as in line 284, 285, and 286. 
       
3. _Adding new data to the Mapbox map:_
    1. _To add a new data type to an existing geographic type:_
        1. If necessary, add a new color function like on line 197 of minimal.html
        2. In the function createColorvector on line 383, add a new variable corresponding to the new data type being added. The output structure of this function is [M-Lab variables, FCC variables, difference variables]. Add the new variable to these sections accordingly.
        3. Depending on what sort of data is being added, you’ll add the name to different variables between lines 875 and 909. If the data will be added to M-Lab, FCC, and their difference, add the variable to “attribute_ids” on line [], to “legend_grouping” on line [], to “legend_dict” on line [], and create “x_labels” and “x_colors” variables with names corresponding to what you add to legend_dict. For variables that are only added to one of M-Lab or FCC, you also need to add in another dictionary element similar to the one in the variable “standalone_admissible_toggles.” Adding standalone variables of this kind is a little trickier because I didn’t quite finish abstracting the process. Currently, “broadband_cutoffs” is hard-coded into a number of places. It shouldn’t be too difficult to abstract away from “broadband_cutoffs” to something that uses a standalone ID based on a toggle. 

    2. _Adding new geometries to the map_
        1. Add ifelse statements to the “loadStandalone” and “loadLayers” functions that use the correct geometries, as in lines 655 and 723
        2. Add the geometry’s ID to “geo_ids” on line 877 and the display name on line 881.
        3. Add the geomtry load just after the map load at line 1026.

		
