#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#|Functions for United States of Broadband Pipeline|#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

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

