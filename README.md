# SOTI
A private repository for the working draft of the United States of Broadband/State of the Internet OTI project

The [maps on the landing page for this project](https://opentechinstitute.github.io/SOTI/SOTI.html) use data that come from the M-Lab NDT 
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

[Add instructions on getting from the output of 2 to the input of 3 and the output of 3 to the input of 4].

Run 4.

That will produce the geojson and json files that the Mapbox map embedded in the project landing page needs. The pipeline also produces 
several other R objects as auxilliary files. These are data files that are used in mapbox_pipeline_wrapper.

As I add more files and make this pipeline more functional, I'll be updating this README. 
