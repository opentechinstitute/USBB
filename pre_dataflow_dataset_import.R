#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#|Script to import and prepare census data for Dataflow|#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#########
#Library#
#########

library(tidyverse)
library(sf)
library(tidycensus)
library(purrr)
library(zipcode)
library(maps)

###Set this to directory on your computer where you'll be working on this project 
setwd("Set this to the directory")

###If pipeline_functions.R is in the same directory as this file, you'll be OK. If not, set this
###to the folder containing it
source("pipeline_functions.R")

#################
#Tract-level I/O#
#################

###This loads shapes and population from tidycensus
us <- unique(fips_codes$state)[1:51]
totalpop_sf <- reduce(
  purrr::map(us, function(x) {
    get_acs(geography = "tract", variables =
              c("B17012_001"), 
            state = x, geometry = TRUE)
  }), 
  rbind
)

names(totalpop_sf)[1]<-"GEOID"
names(totalpop_sf)[2]<-"tract"

###This R object file is used in the Mapbox pipeline
save(totalpop_sf, file="totalpop_sf_tract")

totalpop_sf_dataflow<-totalpop_sf%>%mutate(state=str_split(tract, ",")%>%
                                           lapply(function(x){return(x[3])})
                                           %>%unlist
                                    )

###This is a user-defined function that creates a JSON for the Dataflow spatial join
to_dataflow_string(totalpop_sf_dataflow, "dataflow_mapbox_tract.json")

##################
#County-level I/O#
##################

###This loads shapes and population from tidycensus
totalpop_sf_county <- reduce(
  purrr::map(us, function(x) {
    get_acs(geography = "county", variables =
              c("B17012_001"), 
            state = x, geometry = TRUE)
  }), 
  rbind
)

names(totalpop_sf_county)[1]="county"

###This R object file is used in the Mapbox pipeline. No need for a Dataflow object because the
###the tract run makes it redundant.
save(totalpop_sf_county, file="totalpop_sf_county")

###########################
#Legistlative district I/O#
###########################

cong_lower<-vector(length=50, mode="list")
cong_upper<-vector(length=50, mode="list")
state_fips<-unique(fips_codes$state_code)

###These numbers correspond to D.C. and other territories
state_fips<-state_fips[-c(9,52,53,54,56,57)]

###This loads shapefiles from "maps" because tidycensus hasn't incorporated legistlative
###districts yet
for(i in 1:length(state_fips)){
  a<-tryCatch(state_legislative_districts(state=state_fips[i], house="upper", cb = TRUE),
              error = function(e){return("e")})
  b<-tryCatch(state_legislative_districts(state=state_fips[i], house="lower", cb = TRUE),
              error = function(e){return("e")})
  
  if(length(unlist(a))==1){
    a<-state_legislative_districts(state=state_fips[i], house="upper")
  }
  
  if(length(unlist(b))==1){
    b<-state_legislative_districts(state=state_fips[i], house="lower")
  }
  
  cong_upper[[i]]<-st_as_sf(a)%>%select(STATEFP, GEOID ,geometry )
  cong_upper[[i]]$FUNCSTAT<-"upper"
  
  cong_lower[[i]]<-st_as_sf(b)%>%select(STATEFP, GEOID ,geometry )
  cong_lower[[i]]$FUNCSTAT<-"lower"
  
  cong_lower[[i]]<-st_transform(cong_lower[[i]],"+proj=longlat +datum=WGS84")
  cong_upper[[i]]<-st_transform(cong_upper[[i]],"+proj=longlat +datum=WGS84")
}

upper_rm<-lapply(cong_upper, nrow)%>%lapply(function(x) return(is.null(x)))%>%unlist%>%which
lower_rm<-lapply(cong_lower, nrow)%>%lapply(function(x) return(is.null(x)))%>%unlist%>%which
df <- do.call(rbind, cong_lower)
df1 <- do.call(rbind, cong_upper)
names(df1)[2]<-names(df)[2]
df_final<-rbind(df,df1)

###This R object file is used in the Mapbox pipeline
save(df_final, file = "legislative_mlab")

###A little pre-processing is necessary to get this data the right names and shape for 
###dataflow
data(state.fips)

state_cross<-state.fips[,c(1,6)]%>%
  mutate(state=polyname%>%str_split(":")%>%lapply(function(x)return(x[1]))%>%unlist)%>%
  select(fips, state)%>%distinct

state_cross$fips<-ifelse(nchar(state_cross$fips)==1, str_c("0", state_cross$fips), state_cross$fips)
names(state_cross)[1]<-"STATEFP"
missing_fi<-c("02", "15")
missing_name<-c("alaska","hawaii")
missing_data <- data.frame("STATEFP"=missing_fi, "state"=missing_name)
state_cross<-rbind(state_cross, missing_data)

df_final_lower <- df_final%>%filter(FUNCSTAT=="lower")
df_final_upper <- df_final%>%filter(FUNCSTAT=="upper")

dataflow_lower_df<-left_join(df_final_lower,state_cross)%>%select(GEOID, geometry, state)
dataflow_upper_df<-left_join(df_final_upper,state_cross)%>%select(GEOID, geometry, state)

to_dataflow_string(dataflow_lower_df, "dataflow_mapbox1_lower.json")
to_dataflow_string(dataflow_upper_df, "dataflow_mapbox1_upper.json")