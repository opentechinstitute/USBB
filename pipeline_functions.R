#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#|Functions for United States of Broadband Pipeline|#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

project <- "mlab-sandbox"

###A helper function for getting dataflow strings into the right format
stringify = function(x){
  z=apply(x, 1, paste, collapse = ", ")
  w<-str_c("[ ",z, " ]")
  return("Values"=w)
}

###The principal function for getting dataflow strings into the right format and writing them
to_dataflow_string = function(dataflow_sf, file_name){
  require(sf)
  require(jsonlite)
  library(tools)
  
  dataflow_list <-vector(mode = "list",length=nrow(dataflow_sf))
  
  for(i in 1:length(dataflow_list)){
    val <- tryCatch(stringify(dataflow_sf[i,]$geometry[[1]][[1]][[1]]), error=function(e){return("e")})
    if(val=="e"){
      val<-stringify(dataflow_sf[i,]$geometry[[1]][[1]])
    }
    dataflow_list[[i]]<-list("district"=dataflow_sf[i,]$GEOID, 
                                   "Values"=val,
                                   "state"=str_c(" ",toTitleCase(dataflow_sf[i,]$state))
    )
  }
  
  dataflow_json_lower<-toJSON(dataflow_list, auto_unbox = TRUE)
  x <- fromJSON(dataflow_json_lower) # convert to data.frame
  stream_out(x, file(paste(c(file_name, ".json"), collapse=  "")))
}

###process 477 data
process_477 <-function(D){
  D$med_dl<-as.numeric(D$med_dl)
  D$med_ul<-as.numeric(D$med_ul)
  D$num_con_prov<-as.numeric(D$num_con_prov)
  names(D)[1]<-"GEOID"
  D$GEOID<-as.character(D$GEOID)
  D$GEOID<-as.factor(D$GEOID)
  return(D)
}

process_477_prov <-function(D){
  D$med_dl<-as.numeric(D$med_dl)
  D$Provider_name<-as.character(D$Provider_name)
  names(D)[3]<-"GEOID"
  D$GEOID<-as.character(D$GEOID)
  D$GEOID<-as.factor(D$GEOID)
  return(D)
}

###loading different time chunks
load_477_data<-function(query,table){
  start_loc<-str_locate(query,"TABLE")
  
  final_query<-str_c(str_sub(query, 1, start_loc[1]-1), table, 
        str_sub(query, start_loc[2]+1,nchar(query)), collapse="")
  
  result<-query_exec(final_query, project = project, use_legacy_sql=FALSE, max_pages = Inf)
  
  return(result)
}

load_time_chunks<-function(query){
  
  start_loc<-str_locate(query,"GROUP")
  dec_14<-"WHERE day BETWEEN '2014-07-01' AND '2014-12-01'"
  jun_15<-"WHERE day BETWEEN '2014-12-01' AND '2015-07-01'"
  dec_15<-"WHERE day BETWEEN '2015-07-01' AND '2015-12-01'"
  jun_16<-"WHERE day BETWEEN '2015-12-01' AND '2016-07-01'"
  dec_16<-"WHERE day BETWEEN '2016-07-01' AND '2016-12-01'"
  jun_17<-"WHERE day BETWEEN '2016-12-01' AND '2017-07-01'"
  
  query_list <- list(dec_14=dec_14, jun_15=jun_15,
                     dec_15=dec_15, jun_16=jun_16, dec_16=dec_16,jun_17=jun_17)
  
  full_D<-data.frame()
  
  for(i in 1:length(query_list)){
    query_list[[i]]<-  str_c(str_sub(query, 1, start_loc[1]-2), 
                             query_list[[i]], 
                             str_sub(query, start_loc[1],nchar(query)), collapse="")
    
    curr_D<-query_exec(query_list[[i]], project = project, use_legacy_sql=FALSE, max_pages = Inf)
    full_D<-bind_rows(full_D,data.frame(curr_D, date_range =names(query_list)[i]))
  }
  
  return(full_D)
}
