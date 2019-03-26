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

setwd("~/Desktop/SOTI/")

### setwd("Set this to the directory")
load("MLab_data_census_tract1")
load("MLab_data_census_tract1_up" )
load("D_477_2018_dec_prov")
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

D_477_2018_dec_p<-process_477_prov(D_477_2018_dec_prov)
D_477_2017_jun_p<-process_477_prov(D_477_2017_jun_prov)
D_477_2016_dec_p<-process_477_prov(D_477_2016_dec_prov)
D_477_2016_jun_p<-process_477_prov(D_477_2016_jun_prov)
D_477_2015_dec_p<-process_477_prov(D_477_2015_dec_prov)
D_477_2015_jun_p<-process_477_prov(D_477_2015_jun_prov)
D_477_2014_dec_p<-process_477_prov(D_477_2014_dec_prov)

D_477<-bind_rows(data.frame(D_477_2018_dec_p, date_range="dec_18")
                 )

D_477<-bind_rows(data.frame(D_477_2017_jun_p, date_range="jun_17"),
                 data.frame(D_477_2016_jun_p, date_range="jun_16"),
                 data.frame(D_477_2015_dec_p, date_range="dec_16"),
                 data.frame(D_477_2015_jun_p, date_range="jun_15"),
                 data.frame(D_477_2015_dec_p, date_range="dec_15"),
                 data.frame(D_477_2014_dec_p, date_range="dec_14")
                 )

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

D_joined<-left_join(D, totalpop_sf, by = "tract")
D_joined_up <- left_join(todo_copies_up, totalpop_sf)

D_joined_summed<-D_joined%>%mutate(GEOID=str_sub(GEOID,1,5))%>%group_by(GEOID, date_range)%>%
  summarise(mlab_speed=median(med_speed, na.rm = TRUE),
            counts = sum(count_ip, na.rm = TRUE))
D_joined_summed_up<-D_joined_up%>%mutate(GEOID=str_sub(GEOID,1,5))%>%group_by(GEOID, date_range)%>%
  summarise(mlab_up=median(upload_med_speed, na.rm = TRUE))

D_joined_summed_tog<-left_join(D_joined_summed,D_joined_summed_up, by= c("GEOID","date_range"))

D_mlab_census_shape_summed<-D_477%>%na.omit%>%group_by(GEOID, date_range, Provider_name)%>%
  summarise(speed_477=median(med_dl), speed_477_up = median(med_ul))%>%
  group_by(GEOID, date_range)%>%summarise(speed_477=median(speed_477), 
                                          speed_477_up= median(as.numeric(speed_477_up)))

rm(D_477)

D_mlab_census_shape_477<-left_join(D_mlab_census_shape_summed, D_joined_summed_tog, 
                    by = c("GEOID", "date_range"))%>%mutate(diff =speed_477- mlab_speed,
                                                            diff_up= speed_477_up-mlab_up)

D_plot_f<-left_join(D_mlab_census_shape_477,totalpop_sf_county[,c(1,6)], by = c("GEOID"="county"))

rm(D_joined)
rm(D_mlab_census_shape)
rm(D_mlab_census_shape_477)

#geoms <- D_plot_f[,c(1,7)]%>%na.omit%>%distinct
#simplepolys <- ms_simplify(geoms)%>%st_as_sf
#D_new<-data.frame(geom = simplepolys$geometry, county = simplepolys$county)%>%st_as_sf()

D_plot_f<-D_plot_f%>%mutate(speed_diff_perc = speed_477/mlab_speed, 
                            speed_up_diff_perc=speed_477_up/mlab_up)

D_data_county<-data.frame(county = D_plot_f$GEOID , 
                          speed_mlab=D_plot_f$mlab_speed , 
                          speed_mlab_up=D_plot_f$mlab_up,
                          speed_477=D_plot_f$speed_477, 
                          speed_477_up=D_plot_f$speed_477_up, 
                          speed_diff=D_plot_f$diff,
                          speed_diff_up=D_plot_f$diff_up,
                          speed_diff_perc=D_plot_f$speed_diff_perc,
                          speed_diff_perc_up=D_plot_f$speed_up_diff_perc,
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
### data is compared to FCC data (which can't be joined accurately(), the data from this section 
### is used. 

m_lab_477_final_sf <- D_plot_f%>%st_as_sf
st_crs(m_lab_477_final_sf)<-st_crs(df_final)
df_final<-df_final%>%select(GEOID, geometry, FUNCSTAT)
m_lab_477_final_leg_sf<-st_join(m_lab_477_final_sf,df_final)

m_lab_final_leg_dis<-m_lab_477_final_leg_sf%>%group_by(GEOID.y, FUNCSTAT,date_range)%>%
  summarise(
    speed_477=median(speed_477),
    speed_477_up =median(speed_477_up),
    ### Calculate average speeds, weighted by test counts
    speed_mlab=sum(mlab_speed *counts, na.rm = TRUE)/(sum(counts, na.rm = TRUE)), 
    speed_up_mlab = sum(mlab_up *counts, na.rm = TRUE)/(sum(counts, na.rm = TRUE)), 
    counts_avg = median(counts, na.rm=TRUE)
    
  )%>%mutate(
    speed_diff= speed_477-speed_mlab,
    speed_diff_up= speed_477_up-speed_up_mlab
  )


### This section does the legistlative data by actual spatial joins which makes the mlab 
### accurate but makes comparisons between 477 and mlab not quite even. 

load("MLab_data_state_house_2")
load("MLab_data_state_house_up")
load("MLab_data_state_senate_2")
load("MLab_data_state_senate_up")
load("house_counts_2")
load("senate_counts_2")

D_house<-data.frame(D_state_house, house=rep("lower", nrow(D_state_house)))%>%
  mutate(GEOID=as.character("GEOID"))%>%group_by(GEOID, house,date_range)%>%summarise(
    med_speed =median(med_speed, na.rm=TRUE)
    )
D_house_up<-D_state_house_up%>%mutate(GEOID=as.character("GEOID"))%>%group_by(GEOID,date_range)%>%
  summarise(med_up_speed =median(med_up_speed, na.rm=TRUE) )

day_range<-D_state_house%>%select(day, date_range)%>%distinct

D_house_counts<-left_join(day_range, house_count)%>%group_by(GEOID,date_range)%>%
  summarise(counts = sum(count_ip, na.rm=TRUE))
  
D_house_joined <- left_join(D_house, D_house_counts)
D_house_final <- left_join(D_house_joined, D_house_up, by = c("GEOID","date_range"))

house_shape<-df_final%>%filter(FUNCSTAT=="lower")%>%select(GEOID, geometry)
house_df<-left_join(D_house_final, house_shape)

D_senate<-data.frame(D_state_senate, house=rep("upper", nrow(D_state_senate)))%>%
  group_by(GEOID, house,date_range)%>%summarise(
    med_speed =median(med_speed, na.rm = TRUE)
  )

D_senate_up<-D_state_senate_up%>%group_by(senate_tract,date_range)%>%
  summarise(med_up_speed =median(med_up_speed, na.rm=TRUE) )

day_range<-D_state_senate%>%select(day, date_range)%>%distinct

D_senate_counts<-left_join(day_range, senate_count)%>%group_by(GEOID,date_range)%>%
  summarise(counts = sum(count_ip, na.rm=TRUE))

D_senate_joined <- left_join(D_senate, D_senate_counts)
D_senate_final <- left_join(D_senate_joined, D_senate_up, by = c("GEOID"="senate_tract","date_range"))

senate_shape<-df_final%>%filter(FUNCSTAT=="upper")%>%select(GEOID, geometry)
senate_df<-left_join(D_senate_final, senate_shape)

legis_df <-bind_rows(D_senate_final, D_house_final)
legis_comp_df <-left_join(m_lab_final_leg_dis, legis_df, by = c("GEOID.y"="GEOID","date_range","FUNCSTAT"="house"))%>%
  mutate(speed_diff_perc = speed_477/speed_mlab, speed_diff_perc_up = speed_477/speed_mlab)

############################
#90th quantile calculations#
############################

load("90q_senate")
load("90q_senate_up")
load("90q_county")
load("90q_county_up")
load("90q_senate")
load("90q_house_up")

county_s<-left_join(county, totalpop_sf, by = "tract")%>%select(nine_speed, date_range, GEOID)%>%
  mutate(GEOID=str_sub(GEOID,1,5))%>%group_by(GEOID, date_range)%>%
  summarise(nine_speed=median(nine_speed, na.rm = TRUE))

county_up_s<-left_join(county_up, totalpop_sf, by = "tract")%>%
  select(upload_nine_speed, date_range, GEOID)%>%mutate(GEOID=str_sub(GEOID,1,5))%>%
  group_by(GEOID, date_range)%>%summarise(nine_up_speed=median(upload_nine_speed, na.rm = TRUE))

county_joined<-left_join(county_s, county_up_s, by = c("GEOID","date_range"))
  
house_s<-data.frame(house, FUNCSTAT=rep("lower", nrow(house)))%>%
  select(med_speed, date_range, tract,FUNCSTAT)%>%group_by(tract, date_range,FUNCSTAT)%>%
  summarise(med_speed=median(med_speed, na.rm = TRUE))

house_up_s<-data.frame(house_up, FUNCSTAT=rep("lower", nrow(house_up)))%>%
  select(med_up_speed, date_range, tract,FUNCSTAT)%>%group_by(tract, date_range,FUNCSTAT)%>%
  summarise(med_up_speed=median(med_up_speed, na.rm = TRUE))

house_joined<-left_join(house_s, house_up_s, by = c("tract","date_range"))%>%
  select(tract,date_range, FUNCSTAT=FUNCSTAT.x, nine_speed=med_speed, nine_up_speed=med_up_speed)

senate_s<-data.frame(senate, FUNCSTAT=rep("upper", nrow(senate)))%>%
  select(med_speed, date_range, tract,FUNCSTAT)%>%group_by(tract, date_range,FUNCSTAT)%>%
  summarise(med_speed=median(med_speed, na.rm = TRUE))

senate_up_s<-data.frame(senate_up, FUNCSTAT=rep("upper", nrow(senate_up)))%>%
  select(med_up_speed, date_range, senate_tract,FUNCSTAT)%>%
  group_by(senate_tract, date_range,FUNCSTAT)%>%summarise(med_up_speed=median(med_up_speed, na.rm = TRUE))

senate_joined<-left_join(senate_s, senate_up_s, by = c("tract"="senate_tract","date_range"))%>%
  select(tract,date_range, FUNCSTAT=FUNCSTAT.x, nine_speed=med_speed, nine_up_speed=med_up_speed)

joined_90q_leg <- bind_rows(house_joined, senate_joined)

leg_broadband_cutoff<-joined_90q_leg%>%
  mutate(failed_by=case_when((nine_speed<10)&(nine_up_speed)<1~"Both below cutoffs",
                             nine_speed<10~"Download below 10 Mbps",
                             nine_up_speed<1~"Upload below 1 Mbps",
                             (nine_speed>=10)&(nine_up_speed>=1)~"Both above cutoffs"))

county_broadband_cutoff<-county_joined%>%
  mutate(failed_by=case_when((nine_speed<10)&(nine_up_speed)<1~"Both below cutoffs",
                             nine_speed<10~"Download below 10 Mbps",
                             nine_up_speed<1~"Upload below 1 Mbps",
                             (nine_speed>=10)&(nine_up_speed>=1)~"Both above cutoffs"))

legis_comp_nine_df<-left_join(legis_comp_df, leg_broadband_cutoff, 
                              by =c("GEOID.y"="tract", "FUNCSTAT", "date_range"))

D_data_county_comp<-left_join(D_data_county, county_broadband_cutoff, 
                              by = c("county"="GEOID","date_range"))
names(D_data_county_comp)[14]<-"broadband_cutoffs"

##only for maps
legis_comp_nine_df<-legis_comp_df
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

D_data<-data.frame(house_num=legis_comp_nine_df$GEOID.y, 
                   house=legis_comp_nine_df$FUNCSTAT,
                   speed_mlab=legis_comp_nine_df$speed_mlab,
                   speed_mlab_up=legis_comp_nine_df$speed_up_mlab,
                   speed_477=legis_comp_nine_df$speed_477,
                   speed_477_up=legis_comp_nine_df$speed_477_up,
                   speed_diff=legis_comp_nine_df$speed_diff,
                   speed_diff_up=legis_comp_nine_df$speed_diff_up,
                   speed_diff_perc=legis_comp_nine_df$speed_diff_perc,
                   speed_diff_perc_up=legis_comp_nine_df$speed_diff_perc_up,
                   counts = legis_comp_nine_df$counts,
                   #broadband_cutoffs = legis_comp_nine_df$failed_by,
                   date_range=legis_comp_nine_df$date_range
)

  ### Iterate through years, join the county and legislative JSONS and write out time-chunked JSONS
  D_data_time<-D_data
  D_data_county_time<-D_data_county_comp
  
  leg_json <-toJSON(D_data_time)
  county_json <- toJSON(D_data_county_time)

  writeLines(leg_json, con = "temp_leg.json")
  writeLines(county_json, con = "temp_county.json")
  
  zcta=read_json("temp_county.json")
  leg = read_json("temp_leg.json")
  
  json_list=list(county=zcta, house_=leg)
  data_layer_json<-toJSON(json_list)

  writeLines(data_layer_json, 
             con=paste(c("mapbox_final_json.json"),collapse = "")
  )
  
  
###########################################  
##############mapping loops################
###########################################  
  
  
  
  D_house <- read_feather("D_data.feather")
  D_county <- read_feather("D_data_county.feather")
  
  D_county_PA<-D_county %>% filter(str_sub(county, 1,2)=="42"&date_range==dates[i])
  D_house_PA<-D_house %>% filter(str_sub(house_num, 1,2)=="42"&house=="lower"&date_range==dates[i]) 
  D_senate_PA<-D_house %>% filter(str_sub(house_num, 1,2)=="42"&house=="upper"&date_range==dates[i]) 
  
  rm_inds<-D_county_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which
  rm_inds_house<-D_house_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which
  rm_inds_sen<-D_senate_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which
  
  D_county_PA<-D_county_PA %>% data.frame %>% select(county, date_range, speed_diff)
  D_senate_PA<-D_senate_PA %>% data.frame %>% select(house_num, house, date_range, speed_diff)
  D_house_PA<-D_house_PA %>% data.frame %>% select(house_num, house, date_range, speed_diff)
  
  if(length(rm_inds)!=0){
    D_county_PA<-D_county_PA[-rm_inds,]
  }
  if(length(rm_inds_house)!=0){
    D_house_PA<-D_house_PA[-rm_inds_house,]
  }
  if(length(rm_inds_sen)!=0){
    D_senate_PA<-D_senate_PA[-rm_inds_sen,]
  }
  
  D_county_PA<-D_county_PA %>%  mutate_all(unlist)
  D_house_PA<-D_house_PA %>%  mutate_all(unlist)
  D_senate_PA<-D_senate_PA %>%  mutate_all(unlist)
  
  cuts<-c(0,.2, 4, 10, 25,50,100,1000)
  colors_v<-viridis(7, option="magma")
  
  D_house_PA_j<-D_house_PA %>% left_join(df_final %>% filter(FUNCSTAT=="lower"), by =c("house_num"="GEOID" )) %>% 
    mutate(speed_mlab_c=cut(speed_mlab, cuts)) %>% mutate(
      speed_color = case_when(
        speed_mlab_c=="(0.2,4]"~colors_v[1],
        speed_mlab_c=="(4,10]"~colors_v[2],
        speed_mlab_c=="(10,25]"~colors_v[3],
        speed_mlab_c=="(25,50]"~colors_v[4]
        
        )
      ) %>% mutate(
        speed_labels = case_when(
          speed_mlab_c=="(0.2,4]"~".2 to 4 Mbps",
          speed_mlab_c=="(4,10]"~"4 to 10 Mbps",
          speed_mlab_c=="(10,25]"~"10 to 25 Mbps",
          speed_mlab_c=="(25,50]"~"25 to 50 Mbps"
          
        )
      )
  
  D_senate_PA_j<-D_senate_PA %>% left_join(df_final %>% filter(FUNCSTAT=="upper"), by =c("house_num"="GEOID" ))%>% 
    mutate(speed_mlab_c=cut(speed_mlab, cuts))%>% mutate(
      speed_color = case_when(
        speed_mlab_c=="(0.2,4]"~colors_v[1],
        speed_mlab_c=="(4,10]"~colors_v[2],
        speed_mlab_c=="(10,25]"~colors_v[3],
        speed_mlab_c=="(25,50]"~colors_v[4]
        
      )
    ) %>% mutate(
      speed_labels = case_when(
        speed_mlab_c=="(0.2,4]"~".2 to 4 Mbps",
        speed_mlab_c=="(4,10]"~"4 to 10 Mbps",
        speed_mlab_c=="(10,25]"~"10 to 25 Mbps",
        speed_mlab_c=="(25,50]"~"25 to 50 Mbps"
        
      )
    )
  
  D_county_PA_j<-D_county_PA %>% left_join(totalpop_sf_county , by ="county" ) %>% 
    mutate(NAME=str_split(NAME,",") %>% lapply(function(x)return(x[[1]])))%>% 
    mutate(speed_mlab_c=cut(speed_mlab, cuts))%>% mutate(
      speed_color = case_when(
        speed_mlab_c=="(0.2,4]"~colors_v[1],
        speed_mlab_c=="(4,10]"~colors_v[2],
        speed_mlab_c=="(10,25]"~colors_v[3],
        speed_mlab_c=="(25,50]"~colors_v[4]
        
      )
    ) %>% mutate(
      speed_labels = case_when(
        speed_mlab_c=="(0.2,4]"~".2 to 4 Mbps",
        speed_mlab_c=="(4,10]"~"4 to 10 Mbps",
        speed_mlab_c=="(10,25]"~"10 to 25 Mbps",
        speed_mlab_c=="(25,50]"~"25 to 50 Mbps"
        
      )
    )
  
  ###Large maps
  svg(filename = "PA_state_house.svg")
  D_house_PA_j %>% ggplot(aes(fill=speed_color, color = speed_color))+geom_sf()+
    scale_color_manual(values = viridis(7, option="magma"), 
                       labels = c(".2 to 4 Mbps","4 to 10 Mbps","10 to 25 Mbps","25 to 50 Mbps"),name = "M-Lab DL Speed")+ 
    scale_fill_manual(values = viridis(7, option="magma"),
                      labels=c(".2 to 4 Mbps","4 to 10 Mbps","10 to 25 Mbps","25 to 50 Mbps"),name = "M-Lab DL Speed")+ 
    theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
          axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
          panel.background = element_rect(fill = "white", colour = "white" )
          )+
    labs(title = "Download speed by State House")
  dev.off()
  
  svg(filename = "PA_state_senate.svg")
  D_senate_PA_j %>% ggplot(aes(fill=speed_color, color = speed_color))+geom_sf()+
    scale_color_manual(values = viridis(7, option="magma"), 
                       labels = c(".2 to 4 Mbps","4 to 10 Mbps","10 to 25 Mbps","25 to 50 Mbps"),name = "M-Lab DL Speed")+ 
    scale_fill_manual(values = viridis(7, option="magma"),
                      labels=c(".2 to 4 Mbps","4 to 10 Mbps","10 to 25 Mbps","25 to 50 Mbps"),name = "M-Lab DL Speed")+ 
    theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
          axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
          panel.background = element_rect(fill = "white", colour = "white" )
    )+
    labs(title = "Download speed by State Senate")
  dev.off()
  
  svg("PA_county.svg")
  D_county_PA_j %>% ggplot(aes(fill=speed_color, color = speed_color))+geom_sf()+
    scale_color_manual(values = viridis(7, option="magma"), 
                       labels = c(".2 to 4 Mbps","4 to 10 Mbps","10 to 25 Mbps","25 to 50 Mbps"),name = "M-Lab DL Speed")+ 
    scale_fill_manual(values = viridis(7, option="magma"),
                      labels=c(".2 to 4 Mbps","4 to 10 Mbps","10 to 25 Mbps","25 to 50 Mbps"),name = "M-Lab DL Speed")+ 
    theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
          axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
          panel.background = element_rect(fill = "white", colour = "white" )
    )+
    labs(title = "Download speed by County")
  dev.off()
  
  ###individuals maps
  setwd("~/Desktop/SOTI/map_images/County/")
  
  for(i in 1:nrow(D_county_PA_j)){

    tiff(filename =str_c(D_county_PA_j[i,]$NAME %>% str_replace_all(" ", "_"),"_map.png" ) %>% tolower,
         units="in", width=5, height=5, res=300)
    
    color_i <- D_county_PA_j$speed_color[i]
    label_i <- str_c(D_county_PA_j$speed_mlab[i]," Mbps")
    p<- D_county_PA_j %>% filter(county==D_county_PA_j[i,]$county) %>% ggplot(aes(fill=speed_color, color = speed_color))+
      geom_sf()+scale_color_manual(values = color_i,labels=label_i, name = "M-Lab DL Speed")+ 
      scale_fill_manual(values = color_i,labels=label_i, name = "M-Lab DL Speed")+ 
      theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
            axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
            panel.background = element_rect(fill = "white", colour = "white" )
      )+
      labs(title = str_c("Download speed in ", D_county_PA_j[i,]$NAME, " PA"))
    print(p)
    dev.off()
    
  }
  
  setwd("~/Desktop/SOTI/map_images/State_house/")
  for(i in 1:nrow(D_house_PA_j)){
    tiff(filename =str_c("house_district_",D_house_PA_j[i,]$house_num %>% str_sub(3,5),"_map.png" ) %>% tolower,
         units="in", width=5, height=5, res=300)
    
    color_i <- D_house_PA_j$speed_color[i]
    label_i <- D_house_PA_j$speed_labels[i]
    p<- D_house_PA_j %>% filter(house_num==D_house_PA_j[i,]$house_num) %>% ggplot(aes(fill=speed_color, color = speed_color))+
      geom_sf()+scale_color_manual(values = color_i,labels=label_i, name = "M-Lab DL Speed")+ 
      scale_fill_manual(values = color_i,labels=label_i, name = "M-Lab DL Speed")+ 
      theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
            axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
            panel.background = element_rect(fill = "white", colour = "white" )
      )+
      labs(title = str_c("Download speed in PA House District ", D_house_PA_j[i,]$house_num %>% str_sub(3,5)))
    print(p)
    dev.off()
    
  }
  
  setwd("~/Desktop/SOTI/map_images/State_senate/")
  for(i in 1:nrow(D_senate_PA_j)){
    tiff(filename =str_c("senate_district_",D_senate_PA_j[i,]$house_num %>% str_sub(3,5),"_map.png" ) %>% tolower,
         units="in", width=5, height=5, res=300)
    color_i <- D_senate_PA_j$speed_color[i]
    label_i <- D_senate_PA_j$speed_labels[i]
    p<- D_senate_PA_j %>% filter(house_num==D_senate_PA_j[i,]$house_num) %>% ggplot(aes(fill=speed_color, color = speed_color))+
      geom_sf()+scale_color_manual(values = color_i,labels=label_i, name = "M-Lab DL Speed")+ 
      scale_fill_manual(values = color_i,labels=label_i, name = "M-Lab DL Speed")+ 
      theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
            axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
            panel.background = element_rect(fill = "white", colour = "white" )
      )+
      labs(title = str_c("Download speed in PA Senate District ", D_senate_PA_j[i,]$house_num %>% str_sub(4,5)))
    print(p)
    dev.off()
    
  }

  D_house <- read_feather("D_data.feather")
  D_county <- read_feather("D_data_county.feather")
  ####477 time perdiod difference maps 
  dates<-DD1$county$date_range %>% unlist %>% unique
  
  setwd("~/Desktop/SOTI/map_images/PA_level/")
  
  for(i in 1:length(dates)){
    
    D_county_PA<-D_county %>% filter(str_sub(county, 1,2)=="42"&date_range==dates[i])
    D_house_PA<-D_house %>% filter(str_sub(house_num, 1,2)=="42"&house=="lower"&date_range==dates[i]) 
    D_senate_PA<-D_house %>% filter(str_sub(house_num, 1,2)=="42"&house=="upper"&date_range==dates[i]) 
    
    rm_inds<-D_county_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which
    rm_inds_house<-D_house_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which
    rm_inds_sen<-D_senate_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which

    D_county_PA<-D_county_PA %>% data.frame %>% select(county, date_range, speed_diff)
    D_senate_PA<-D_senate_PA %>% data.frame %>% select(house_num, house, date_range, speed_diff)
    D_house_PA<-D_house_PA %>% data.frame %>% select(house_num, house, date_range, speed_diff)
    
    if(length(rm_inds)!=0){
      D_county_PA<-D_county_PA[-rm_inds,]
    }
    if(length(rm_inds_house)!=0){
      D_house_PA<-D_house_PA[-rm_inds_house,]
    }
    if(length(rm_inds_sen)!=0){
      D_senate_PA<-D_senate_PA[-rm_inds_sen,]
    }

    D_county_PA<-D_county_PA %>%  mutate_all(unlist)
    D_house_PA<-D_house_PA %>%  mutate_all(unlist)
    D_senate_PA<-D_senate_PA %>%  mutate_all(unlist)
  
    D_senate_PA<-df_final %>% filter(FUNCSTAT=="upper"&str_sub(GEOID,1,2)=="42") %>% 
                                       left_join(D_senate_PA, by = c("GEOID"="house_num" ))
    
    D_house_PA<-df_final %>% filter(FUNCSTAT=="lower"&str_sub(GEOID,1,2)=="42") %>% 
      left_join(D_house_PA, by = c("GEOID"="house_num" ))
    
    cuts<-c(-1000, -25, -15, -5,-1, 1, 5,15,25,1000)
    colors_v<- c('#01665e', '#35978f', '#80cdc1', '#c7eae5', '#f5f5f5', '#f6e8c3', '#dfc27d', '#bf812d', '#8c510a')
    
    D_house_PA_j<-D_house_PA  %>% mutate(speed_mlab_c=cut(speed_diff, cuts)) %>% mutate(
        speed_color = case_when(
          speed_mlab_c=="(-1e+03,-25]"~colors_v[1],
          speed_mlab_c=="(-25,-15]"~colors_v[2],
          speed_mlab_c=="(-15,-5]"~colors_v[3],
          speed_mlab_c=="(-5,-1]"~colors_v[4],
          speed_mlab_c=="(-1,1]"~colors_v[5],
          speed_mlab_c=="(1,5]"~colors_v[6],
          speed_mlab_c=="(5,15]"~colors_v[7],
          speed_mlab_c=="(15,25]"~colors_v[8],
          speed_mlab_c=="(25,1e+03]"~colors_v[9]
        )
      )%>% mutate(
        speed_labels = case_when(
          speed_mlab_c=="(-1e+03,-25]"~"-25 and less",
          speed_mlab_c=="(-25,-15]"~"-25 to -15",
          speed_mlab_c=="(-15,-5]"~"-15 to -5",
          speed_mlab_c=="(-5,-1]"~"-5 to -1",
          speed_mlab_c=="(-1,1]"~"-1 to 1",
          speed_mlab_c=="(1,5]"~"1 to 5",
          speed_mlab_c=="(5,15]"~"5 to 15",
          speed_mlab_c=="(15,25]"~"15 to 25",
          speed_mlab_c=="(25,1e+03]"~"25 and greater"
        )
      )
    
    D_senate_PA_j<-D_senate_PA %>% 
      mutate(speed_mlab_c=cut(speed_diff, cuts))%>% mutate(
        speed_color = case_when(
          speed_mlab_c=="(-1e+03,-25]"~colors_v[1],
          speed_mlab_c=="(-25,-15]"~colors_v[2],
          speed_mlab_c=="(-15,-5]"~colors_v[3],
          speed_mlab_c=="(-5,-1]"~colors_v[4],
          speed_mlab_c=="(-1,1]"~colors_v[5],
          speed_mlab_c=="(1,5]"~colors_v[6],
          speed_mlab_c=="(5,15]"~colors_v[7],
          speed_mlab_c=="(15,25]"~colors_v[8],
          speed_mlab_c=="(25,1e+03]"~colors_v[9]
        )
      )%>% mutate(
        speed_labels = case_when(
          speed_mlab_c=="(-1e+03,-25]"~"-25 and less",
          speed_mlab_c=="(-25,-15]"~"-25 to -15",
          speed_mlab_c=="(-15,-5]"~"-15 to -5",
          speed_mlab_c=="(-5,-1]"~"-5 to -1",
          speed_mlab_c=="(-1,1]"~"-1 to 1",
          speed_mlab_c=="(1,5]"~"1 to 5",
          speed_mlab_c=="(5,15]"~"5 to 15",
          speed_mlab_c=="(15,25]"~"15 to 25",
          speed_mlab_c=="(25,1e+03]"~"25 and greater"
        )
      )
    
    D_county_PA_j<-D_county_PA %>% left_join(totalpop_sf_county , by ="county" ) %>% 
      mutate(NAME=str_split(NAME,",") %>% lapply(function(x)return(x[[1]])))%>% 
      mutate(speed_mlab_c=cut(speed_diff, cuts))%>% mutate(
        speed_color = case_when(
          speed_mlab_c=="(-1e+03,-25]"~colors_v[1],
          speed_mlab_c=="(-25,-15]"~colors_v[2],
          speed_mlab_c=="(-15,-5]"~colors_v[3],
          speed_mlab_c=="(-5,-1]"~colors_v[4],
          speed_mlab_c=="(-1,1]"~colors_v[5],
          speed_mlab_c=="(1,5]"~colors_v[6],
          speed_mlab_c=="(5,15]"~colors_v[7],
          speed_mlab_c=="(15,25]"~colors_v[8],
          speed_mlab_c=="(25,1e+03]"~colors_v[9]
        )
      )%>% mutate(
        speed_labels = case_when(
          speed_mlab_c=="(-1e+03,-25]"~"-25 and less",
          speed_mlab_c=="(-25,-15]"~"-25 to -15",
          speed_mlab_c=="(-15,-5]"~"-15 to -5",
          speed_mlab_c=="(-5,-1]"~"-5 to -1",
          speed_mlab_c=="(-1,1]"~"-1 to 1",
          speed_mlab_c=="(1,5]"~"1 to 5",
          speed_mlab_c=="(5,15]"~"5 to 15",
          speed_mlab_c=="(15,25]"~"15 to 25",
          speed_mlab_c=="(25,1e+03]"~"25 and greater"
        )
      )
    
    D_color<-D_county_PA_j %>% select(speed_color, speed_labels) %>% unique
    orders<-D_color$speed_labels %>% str_split(" ") %>% lapply(function(x) return(x[1])) %>% unlist %>% as.numeric%>% order
    D_color<-D_color[orders,]
    
    tiff(filename =str_c("PA_county_",D_county_PA_j$date_range %>% unique,"_map.tiff" ) %>% tolower,
         units="in", width=5, height=5, res=300)
    
    p<-D_county_PA_j %>% ggplot(aes(fill=speed_color), color = "#lightgrey")+geom_sf()+
      scale_fill_manual(values = D_color$speed_color, 
                        labels =  D_color$speed_labels, name ="")+ 
      theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
            axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
            panel.background = element_rect(fill = "white", colour = "white" )
      )+
      labs(title = "Difference in download speed by county")
    
    print(p)
    dev.off()
    
    D_color<-D_senate_PA_j %>% data.frame%>% select(speed_color, speed_labels) %>% unique
    orders<-D_color$speed_labels %>% str_split(" ") %>% lapply(function(x) return(x[1])) %>% unlist %>% as.numeric %>% order
    D_color<-D_color[orders,]
    
    tiff(filename =str_c("PA_senate_district_",D_county_PA_j$date_range %>% unique,"_map.tiff" ) %>% tolower,
         units="in", width=5, height=5, res=300)
    
    p<-D_senate_PA_j %>% ggplot(aes(fill=speed_color), color = "#lightgrey")+geom_sf()+ 
      scale_fill_manual(values = D_color$speed_color, 
                        labels =  D_color$speed_labels, name ="")+ 
      theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
            axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
            panel.background = element_rect(fill = "white", colour = "white" )
      )+
      labs(title = "Difference in download speed by State Senate district")
    print(p)
    dev.off()
    
    D_color<-D_house_PA_j %>% data.frame%>% select(speed_color, speed_labels) %>% unique
    orders<-D_color$speed_labels %>% str_split(" ") %>% lapply(function(x) return(x[1])) %>% unlist %>% as.numeric %>% order
    D_color<-D_color[orders,]
    
    tiff(filename =str_c("PA_house_",D_county_PA_j$date_range %>% unique,"_map.tiff" ) %>% tolower,
         units="in", width=5, height=5, res=300)
    
    p<-D_house_PA_j %>% ggplot(aes(fill=speed_color), color = "lightgrey")+geom_sf()+
      scale_fill_manual(values = D_color$speed_color, 
                        labels =  D_color$speed_labels, name ="")+ 
      theme(axis.title.x=element_blank(), axis.text.x=element_blank(),axis.ticks.x=element_blank(),
            axis.title.y=element_blank(), axis.text.y=element_blank(),axis.ticks.y=element_blank(),
            panel.background = element_rect(fill = "white", colour = "white" )
      )+
      labs(title = "Difference in download speed by State House district")
    
    print(p)
    dev.off()
    
  }
  
  D_county_PA<-DD1$county %>% select(county, speed_mlab, speed_477, speed_diff, date_range) %>% 
    filter(str_sub(county, 1,2)=="42")
  
  rm_inds<-  D_county_PA$speed_diff %>% lapply(function(x)return(is.null(x))) %>% unlist %>% which
  
  if(length(rm_inds)!=0){
    D_county_PA<-D_county_PA[-rm_inds,]
  }
  
  D_county_PA<-D_county_PA %>%  mutate_all(unlist)
 
  D_county_PA %>% group_by(county, date_range) %>% summarise(NDT_value=median(speed_mlab), Value_477=median(speed_477),
                                                            Difference = median(speed_diff))

  ###########
  ###table###
  ###########
  
  D_d<-D_data %>% select(GEOID=house_num,house, date_range, speed_mlab, speed_477, speed_diff) 
  D_d_c<-D_data_county%>% mutate(house="county") %>% 
    select(GEOID=county, house, date_range, speed_mlab, speed_477, speed_diff) 
  
  D_for_table<-rbind(D_d, D_d_c)
  D_for_table %>% select(house, date_range, GEOID, speed_mlab, speed_477, speed_diff) %>%
    filter(str_sub(GEOID, 1,2)=="42") %>% group_by(GEOID,house, date_range) %>% 
    summarise(NDT_value=median(speed_mlab), Value_477=median(speed_477),Difference = median(speed_diff)) %>%
    write_csv(path = "Speed_table.csv")

###############
###point map###
###############  
  
locs_q<-"#standardSQL
SELECT * 
  
  FROM 
`thieme.US_loc`

WHERE 
state = 'PA' OR
state = 'Pennsylvania'"
project <- "mlab-sandbox"

D_locs<-query_exec(locs_q,project = project, use_legacy_sql=FALSE, max_pages = Inf)
PA_county<-totalpop_sf_county %>% mutate(county=str_sub(county,1,2)) %>% filter(county == "42")
D_points<-D_locs %>% select(lat = latitude, lon=longitude) %>%  st_as_sf(coords=c( "lon","lat"))
st_crs(D_points)<-st_crs(PA_county)

tiff(filename="point_locations_PA.tiff" ,units="in", width=5, height=5, res=300)

 PA_county%>% ggplot()+geom_sf(fill="white", color = "#cdcdcd")+geom_sf(data= D_points, size=.1, color = "#808080")+
  theme(panel.background = element_rect(color = "white", fill = "white"))+
  labs(title="Test locations in PA from 2009 through 2018")
 
 dev.off()

