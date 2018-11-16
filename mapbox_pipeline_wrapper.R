##################Pipeline 
library(bigrquery)
library(tidyverse)
library(lubridate)
library(tigris)
library(tidycensus)
library(sf)
library(rmapshaper)

setwd("C:/users/nickt/desktop/mlab_interactive_client_server/")



################################
#THIS GETS THE TRACT LEVEL DATA#
################################

load("MLab_data_census_tract")
load("477_data")
load("MLab_data_state_house")
load("MLab_data_state_senate")


#change max to copies below to get averages or todo_avg_max to get the averaged maxes
names(D_477)[1]<-"GEOID"
D_477$GEOID<-as.character(D_477$GEOID)
D_477$GEOID<-as.factor(D_477$GEOID)
D_joined<-left_join(todo_copies, totalpop_sf, by = "tract")

##
D_mlab_census_shape<-left_join(D_477, totalpop_sf)%>%na.omit
D_mlab_census_shape_477<-left_join(D_mlab_census_shape, D_joined, by = "GEOID")

D_plot_f<-D_mlab_census_shape_477[,c(1,3,9,10,11,12,13,14)]
geoms <- D_plot_f[,c(1,3)]%>%na.omit%>%distinct

geojson_mlab<-D_plot_f%>%filter(between(day, ymd("2016-06-01"), ymd("2017-07-15")))%>%
  group_by(GEOID)%>%
  summarise(mlab_speed=median(med_speed, na.rm = TRUE), 
            counts = sum(tract_test_counts, na.rm = TRUE))
geojson_m<-left_join(data.frame(GEOID=D_plot_f$GEOID, med_dl=D_plot_f$med_dl), geojson_mlab)%>%
  distinct
geojson_final <-left_join(geojson_m, geoms, by = "GEOID")

mlab_speeds <-st_as_sf(geojson_final)

####################################################################
#MLAB SPEEDS HAS THE TRACT DATA. THE NEXT SECTION DOES COUNTY LEVEL#
####################################################################

###condense into zcta
m_lab_477_final_county <- mlab_speeds%>%mutate(county = str_sub(GEOID, 1,5))
uniq_county <- unique(m_lab_477_final_county$county)
county <- m_lab_477_final_county$county

m_lab_477_final_county_grouped<-m_lab_477_final_county%>%group_by(county)%>%
  summarise(
    med_dl=median(med_dl,na.rm=TRUE), 
    mlab_speed=median(mlab_speed,na.rm=TRUE),
    counts = sum(na.omit(counts)))%>%
  mutate(diff =med_dl- mlab_speed)


D<-left_join(totalpop_sf_county,as.data.frame(m_lab_477_final_county_grouped)[,-6],by ="county")

####splitting the county data into different layers
D_plot_sf<-st_as_sf(D[,c(1,10)])
simplepolys <- ms_simplify(D_plot_sf)%>%st_as_sf

D_new<-data.frame(geom = simplepolys$geometry, county = simplepolys$county)%>%st_as_sf()
st_write(D_new, "mapbox_geometry_county.geojson")

#use for zcta
names(D)<-c("county","a","a","a","a","speed_477","speed_mlab","counts","speed_diff","geom")
st_geometry(D) <- "geom"

D_data<-data.frame(county = D$county, speed_mlab=D$speed_mlab, speed_477=D$speed_477, 
                   speed_diff=D$speed_diff, counts = D$counts)
write_json(D_data, path = "mapbox_data_county.json")




#######################
#NOW JOIN THE DATASETS#
#######################

m_lab_477_final_sf <- st_as_sf(mlab_speeds)

st_crs(m_lab_477_final_sf)<-st_crs(df_final)
m_lab_477_final_leg_sf<-st_join(m_lab_477_final_sf, df_final)


m_lab_final_leg_dis<-m_lab_477_final_leg_sf%>%group_by(GEOID.y, FUNCSTAT)%>%
  summarise(
    med_dl.x=mean(med_dl.x),
    med_dl.y=sum(na.omit(med_dl.y*counts))/(sum(na.omit(counts))),
    counts=sum(na.omit(counts)),
    house = FUNCSTAT[1]
  )%>%mutate(
    diff= med_dl.x-med_dl.y
  )


D_state_house_med<-D_state_house%>%group_by(GEOID)%>%summarise(med = median(med_speed))
D_states<-left_join(dataflow_lower_df, D_state_house_med)
