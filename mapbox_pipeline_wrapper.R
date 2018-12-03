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

#D_477<-D_477[,c(1,2,3,4,5)]
names(D_477)[1]<-"GEOID"

D_joined<-left_join(D, totalpop_sf, by = "tract")
rm(D)
D_mlab_census_shape<-left_join(D_477, totalpop_sf_county, by = c("GEOID"="county"))%>%na.omit
rm(D_477)
D_joined<-D_joined%>%mutate(GEOID=str_sub(GEOID,1,5))
D_mlab_census_shape_477<-left_join(D_mlab_census_shape, D_joined, by = c("GEOID", "date_range"))
rm(D_joined)
rm(D_mlab_census_shape)
D_plot_f<-D_mlab_census_shape_477[,c(1,2,3,4,9,10,11,13,16)]
rm(D_mlab_census_shape_477)

geoms <- D_plot_f[,c(3,5)]%>%na.omit%>%distinct

geojson_mlab<-D_plot_f%>%as.data.frame%>%
  select(GEOID,date_range,med_dl,count_ip,med_speed,count_ip,Provider_name)%>%
  group_by(GEOID, date_range, Provider_name)%>%
  summarise(mlab_speed=median(med_speed, na.rm = TRUE), counts = sum(count_ip, na.rm = TRUE),
            speed_477=median(med_dl))%>%group_by(GEOID, date_range)%>%
  summarise(mlab_speed=median(mlab_speed), counts = sum(counts),speed_477=median(speed_477))%>%
  mutate(diff =speed_477- mlab_speed)

geojson_county_final<-left_join(geoms, geojson_mlab)%>%st_as_sf
rm(geojson_mlab)
simplepolys <- ms_simplify(geoms)%>%st_as_sf
D_new<-data.frame(geom = simplepolys$geometry, county = simplepolys$county)%>%st_as_sf()

###This is a hack to give names to the variables that need them.
names(geojson_county_final)<-c("county","date_range","speed_mlab","counts","speed_477","speed_diff","geom")
st_geometry(geojson_county_final) <- "geom"

geojson_county_final<-geojson_county_final%>%mutate(speed_diff_perc = speed_477/speed_mlab)

D_data_county<-data.frame(county = geojson_county_final$county, 
                          speed_mlab=geojson_county_final$speed_mlab, 
                          speed_477=geojson_county_final$speed_477, 
                          speed_diff=geojson_county_final$speed_diff,
                          speed_diff_perc=geojson_county_final$speed_diff_perc,
                          counts = geojson_county_final$counts, 
                          date_range = geojson_county_final$date_range)



#############################################################
#Prepare state legislature data and spatially join with M-lab#
##############################################################

###this does the legislative data according to the crosswalk files which don't
###map perfectly but allow for even comparison of mlab and 477. This is used as a starting point
###but it's augmented with the more accurate data below. 

###Basically, we know the M-lab data can be joined without heuristics. So where M-lab data is 
###presented alone, the more accurate code below is what produces the information. When M-lab 
###data is compared toFCC data (which can't be joined accurately(), the data from this section 
###is used. 

m_lab_477_final_sf <- geojson_county_final
st_crs(m_lab_477_final_sf)<-st_crs(df_final)
m_lab_477_final_leg_sf<-st_join(m_lab_477_final_sf, df_final)

m_lab_final_leg_dis<-m_lab_477_final_leg_sf%>%group_by(GEOID, FUNCSTAT,date_range)%>%
  summarise(
    speed_477=median(speed_477),
    
    ###this looks weird but it's doing averages weighted by counts
    speed_mlab=sum(na.omit(speed_mlab*counts))/(sum(na.omit(counts))), 
    counts=sum(na.omit(counts)),
    house = FUNCSTAT[1]
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

###this section does the legistlative data by actual spatial joins which makes the mlab 
###accurate but makes comparisons between 477 and mlab not quite even. 
load("MLab_data_state_house")
load("MLab_data_state_senate")

D_house<-data.frame(D_house_sum, house=rep("lower", nrow(D_house_sum)))%>%
  group_by(GEOID, house, date_range)%>%summarise(
    med_speed =median(med_speed, rm.na=TRUE),
    counts =sum(na.omit(count_ip))
    )

house_shape<-df_final%>%filter(FUNCSTAT=="lower")%>%select(GEOID, geometry)
house_df<-left_join(D_house, house_shape)

D_senate<-data.frame(D_senate_sum, house=rep("upper", nrow(D_senate_sum)))%>%
group_by(GEOID, house, date_range)%>%summarise(
  med_speed =median(med_speed, rm.na=TRUE),
  counts =sum(na.omit(count_ip))
  )

senate_shape<-df_final%>%filter(FUNCSTAT=="upper")%>%select(GEOID, geometry)
senate_df<-left_join(D_senate, senate_shape)

legis_df <-bind_rows(senate_df, house_df)
legis_comp_df <-left_join(m_lab_final_leg_dis, legis_df, by = c("GEOID","date_range","house"))%>%
  mutate(speed_diff_perc = speed_477/speed_mlab)


#########################
#Output the Mapbox jsons#
#########################

##these are the geojsons that go to Mapbox to build the shapes. They should be run through 
##tippecanoe after they leave here. The first two are the legislative districts. The last one
## is the county shapes. Together they make up the map layer. The map layer gets hosted at 
##Mapbox

st_write(D_lower, "full_speed_data_census_leg_low.geojson")
st_write(D_upper, "full_speed_data_census_leg_up.geojson")
st_write(D_new, "full_speed_data_census_base_use.geojson")

##This section makes up the data layer.
st_geometry(legis_comp_df) <- "geom"
D_data<-data.frame(house_num=legis_comp_df$GEOID, 
                   house=legis_comp_df$FUNCSTAT,
                   speed_mlab=legis_comp_df$med_speed, 
                   speed_477=legis_comp_df$speed_477, 
                   speed_diff=legis_comp_df$speed_diff,
                   speed_diff_perc=legis_comp_df$speed_diff_perc,
                   counts = legis_comp_df$counts.y,
                   date_range=legis_comp_df$date_range
)

##Iterate through years, join the county and legislative JSONS, and write out time-chunked JSONS
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

