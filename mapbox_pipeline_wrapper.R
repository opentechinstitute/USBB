#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#|Pipeline for the United States of Broadband Mapbox map|#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#########################
#Load libraries and data#
#########################

library(bigrquery)
library(tidyverse)
library(lubridate)
library(tigris)
library(tidycensus)
library(sf)
library(rmapshaper)
library(lubridate)
library(rmapshaper)
library(jsonlite)

setwd("C:/Users/nickt/Desktop/USB_folder")
### setwd("Set this to the directory")
load("MLab_data_census_tract")
load("D_477_2017_jun_prov")
load("D_477_2016_dec_prov")
load("D_477_2016_jun_prov")
load("D_477_2015_dec_prov")
load("D_477_2015_jun_prov")
load("D_477_2014_dec_prov")
load("totalpop_sf_county")
load("totalpop_sf_tract")
load("legislative_mlab")
source("pipeline_functions.R")

##################################
#Prepare census county-level data#
##################################

D_477_2017_jun_p<-process_477_prov(D_477_2017_jun_prov)
D_477_2016_dec_p<-process_477_prov(D_477_2016_dec_prov)
D_477_2016_jun_p<-process_477_prov(D_477_2016_jun_prov)
D_477_2015_dec_p<-process_477_prov(D_477_2015_dec_prov)
D_477_2015_jun_p<-process_477_prov(D_477_2015_jun_prov)
D_477_2014_dec_p<-process_477_prov(D_477_2014_dec_prov)

D_477<-bind_rows(data.frame(D_477_2017_jun_p, date_range="jun_17"),
                 data.frame(D_477_2016_dec_p,date_range="dec_16"),
                 data.frame(D_477_2016_jun_p,date_range="jun_16"),
                 data.frame(D_477_2015_dec_p,date_range="dec_15"),
                 data.frame(D_477_2015_jun_p,date_range="jun_15"),
                 data.frame(D_477_2014_dec_p,date_range="dec_14"))

rm(D_477_2017_jun_p)
rm(D_477_2016_dec_p)
rm(D_477_2016_jun_p)
rm(D_477_2015_dec_p)
rm(D_477_2015_jun_p)
rm(D_477_2014_dec_p)
rm(D_477_2017_jun_prov)
rm(D_477_2016_dec_prov)
rm(D_477_2016_jun_prov)
rm(D_477_2015_dec_prov)
rm(D_477_2015_jun_prov)
rm(D_477_2014_dec_prov)

names(D_477)[3]<-"GEOID"

D_joined<-left_join(D, totalpop_sf, by = "tract")

D_joined_summed<-D_joined%>%mutate(GEOID=str_sub(GEOID,1,5))%>%group_by(GEOID, date_range)%>%
  summarise(mlab_speed=median(med_speed, na.rm = TRUE), counts = sum(count_ip, na.rm = TRUE))

rm(D)

D_mlab_census_shape_summed<-D_477%>%na.omit%>%group_by(GEOID, date_range, Provider_name)%>%
  summarise(speed_477=median(med_dl))%>%group_by(GEOID, date_range)%>%
  summarise(speed_477=median(speed_477))

rm(D_477)

D_mlab_census_shape_477<-left_join(D_mlab_census_shape_summed, D_joined_summed, 
                    by = c("GEOID", "date_range"))%>%mutate(diff =speed_477- mlab_speed)

D_plot_f<-left_join(D_mlab_census_shape_477,totalpop_sf_county[,c(1,6)], by = c("GEOID"="county"))

rm(D_joined)
rm(D_mlab_census_shape)
rm(D_mlab_census_shape_477)

geoms <- D_plot_f[,c(1,7)]%>%na.omit%>%distinct
simplepolys <- ms_simplify(geoms)%>%st_as_sf

D_new<-data.frame(geom = simplepolys$geometry, county = simplepolys$county)%>%st_as_sf()

D_plot_f<-D_plot_f%>%mutate(speed_diff_perc = speed_477/mlab_speed)

D_data_county<-data.frame(county = D_plot_f$GEOID , 
                          speed_mlab=D_plot_f$mlab_speed , 
                          speed_477=D_plot_f$speed_477, 
                          speed_diff=D_plot_f$diff,
                          speed_diff_perc=D_plot_f$speed_diff_perc,
                          counts = D_plot_f$counts, 
                          date_range = D_plot_f$date_range)

#############################################################
#Prepare state legislature data and spatially join with M-lab#
##############################################################

### This does the legislative data according to the crosswalk files which don't map perfectly 
### but allow for even comparison of mlab and 477. This is used as a starting point but it's 
### augmented with the more accurate data below. 

### Basically, we know the M-lab data can be joined without heuristics. So where M-lab data is 
### presented alone, the more accurate code below is what produces the information. When M-lab 
### data is compared toFCC data (which can't be joined accurately(), the data from this section 
### is used. 

m_lab_477_final_sf <- D_plot_f%>%st_as_sf
st_crs(m_lab_477_final_sf)<-st_crs(df_final)
df_final<-df_final%>%select(GEOID, geometry, FUNCSTAT)
m_lab_477_final_leg_sf<-st_join(m_lab_477_final_sf,df_final)

m_lab_final_leg_dis<-m_lab_477_final_leg_sf%>%group_by(GEOID.y, FUNCSTAT,date_range)%>%
  summarise(
    speed_477=median(speed_477),
    ### Calculate average speeds, weighted by test counts
    speed_mlab=sum(na.omit(mlab_speed *counts))/(sum(na.omit(counts))), 
    
  )%>%mutate(
    speed_diff= speed_477-speed_mlab
  )

simplepolys <- ms_simplify(m_lab_final_leg_dis)%>%st_as_sf

D_lower<-data.frame(geom = simplepolys$geom, house=simplepolys$FUNCSTAT,
                    house_num = simplepolys$GEOID)%>%
  filter(as.character(house)=="lower")%>%st_as_sf()

D_upper<-data.frame(geom = simplepolys$geom, house=simplepolys$FUNCSTAT,
                    house_num = simplepolys$GEOID)%>%
  filter(house=="upper")%>%st_as_sf()

### This section does the legistlative data by actual spatial joins which makes the mlab 
### accurate but makes comparisons between 477 and mlab not quite even. 

load("MLab_data_state_house")
load("MLab_data_state_senate")
load("house_counts")
load("senate_counts")

D_house<-data.frame(D_state_house, house=rep("lower", nrow(D_state_house)))%>%
  group_by(GEOID, house,date_range)%>%summarise(
    med_speed =median(med_speed, rm.na=TRUE)
    )

day_range<-D_state_house%>%select(day, date_range)%>%distinct

D_house_counts<-left_join(day_range, house_count)%>%group_by(GEOID,date_range)%>%
  summarise(counts = sum(count_ip, rm.na=TRUE))
  
D_house_joined <- left_join(D_house, D_house_counts)

house_shape<-df_final%>%filter(FUNCSTAT=="lower")%>%select(GEOID, geometry)
house_df<-left_join(D_house_joined, house_shape)

D_senate<-data.frame(D_state_senate, house=rep("upper", nrow(D_state_senate)))%>%
  group_by(GEOID, house,date_range)%>%summarise(
    med_speed =median(med_speed, rm.na=TRUE)
  )

day_range<-D_state_senate%>%select(day, date_range)%>%distinct

D_senate_counts<-left_join(day_range, senate_count)%>%group_by(GEOID,date_range)%>%
  summarise(counts = sum(count_ip, rm.na=TRUE))

D_senate_joined <- left_join(D_senate, D_senate_counts)

legis_df <-bind_rows(D_senate_joined, D_house_joined)
legis_comp_df <-left_join(m_lab_final_leg_dis, legis_df, by = c("GEOID.y"="GEOID","date_range","FUNCSTAT"="house"))%>%
  mutate(speed_diff_perc = speed_477/speed_mlab)


#########################
#Output the Mapbox jsons#
#########################

## These are the geojsons that go to Mapbox to build the shapes. They should be run through 
## tippecanoe after they leave here. The first two are the legislative districts. The last one
## is the county shapes. Together they make up the map layer. The map layer gets hosted at 
## Mapbox

st_write(D_lower, "full_speed_data_census_leg_low.geojson")
st_write(D_upper, "full_speed_data_census_leg_up.geojson")
st_write(D_new, "full_speed_data_census_base_use.geojson")

### This section makes up the data layer.

st_geometry(legis_comp_df) <- "geom"
D_data<-data.frame(house_num=legis_comp_df$GEOID.y, 
                   house=legis_comp_df$FUNCSTAT,
                   speed_mlab=legis_comp_df$med_speed, 
                   speed_477=legis_comp_df$speed_477, 
                   speed_diff=legis_comp_df$speed_diff,
                   speed_diff_perc=legis_comp_df$speed_diff_perc,
                   counts = legis_comp_df$counts,
                   date_range=legis_comp_df$date_range
)

### Iterate through years, join the county and legislative JSONS and write out time-chunked JSONS
  D_data_time<-D_data
  D_data_county_time<-D_data_county
  
  leg_json <-toJSON(D_data_time)
  county_json <- toJSON(D_data_county_time)

  writeLines(leg_json, con = "temp_leg.json")
  writeLines(county_json, con = "temp_county.json")
  
  zcta=read_json("temp_county.json")
  leg = read_json("temp_leg.json")
  
  json_list=list(county=zcta, house_=leg)
  data_layer_json<-toJSON(json_list)

  writeLines(data_layer_json, 
             con=paste(c("mapbox_leg_county_counts.json"),collapse = "")
  )

