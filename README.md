# SOTI
A private repository for the working draft of the United States of Broadband/State of the Internet OTI project

The [maps on the landing page for this project](https://opentechinstitute.github.io/SOTI/SOTI.html) use data that come from the M-Lab NDT 
tables in BigQuery. However, to get them in the right format and join them with census tract data, they get processed by Dataflow and 
aggregated in R. The code for that pipeline is in this repository. 

There are three scripts in here and one function file.

The three scripts are:

pre_dataflow_dataset_import, which gets census data and writes it out in the right format for the Python dataflow script,

post_dataflow_dataset_import, which gets the joined data from BigQuery after the dataflow script has run,

mapbox_pipeline_wrapper, which processes the BigQuery data and produces the geojson map layers and json data layer for Mapbox.

To use this pipeline, download the four files in this repo and put them in the same folder. 
Open them in R and run them in the order:

1) pre_dataflow_dataset_import
2) [Dataflow script that needs to be added]
3) post_dataflow_dataset_import
4) mapbox_pipeline_wrapper

That will produce the geojson and json files that the Mapbox map embedded in the project landing page needs. The pipeline also produces 
several other R objects as auxilliary files. These are data files that are used in mapbox_pipeline_wrapper.
