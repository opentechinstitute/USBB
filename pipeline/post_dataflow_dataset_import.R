#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#|Script to import spatially-joined data from BigQuery|#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#########
#Library#
#########

library(bigrquery)
project <- "mlab-sandbox"
load("legislative_mlab")

##################
#BigQuery queries#
##################

query_agg_ip<-"#standardSQL

SELECT day,
APPROX_QUANTILES(med_rtt, 1000)[OFFSET(500)] as med_rtt,
APPROX_QUANTILES(med_speed, 1000)[OFFSET(500)] as med_speed,
SUM(count_ip) as tract_test_counts, tract
FROM
`oti_usob.Aggregated_MLab_DL_census_new`

GROUP BY
day, tract"

query_agg_up<-"#standardSQL

SELECT day,
APPROX_QUANTILES(upload_med_speed, 1000)[OFFSET(500)] as upload_med_speed,
SUM(count_ip) as tract_test_counts, tract
FROM
`oti_usob.Aggregated_MLab_UL_census_new`

GROUP BY
day, tract"

query_house<-"#standardSQL

SELECT day,
APPROX_QUANTILES(med_rtt, 1000)[OFFSET(500)] as med_rtt,
APPROX_QUANTILES(med_speed, 1000)[OFFSET(500)] as med_speed,
SUM(count_ip) as tract_test_counts,
client_lat, client_lon, tract
FROM
`oti_usob.Aggregated_MLab_DL_state_house_new`

GROUP BY
client_lat, client_lon, day, tract"

query_house_up<-"#standardSQL

SELECT day,
APPROX_QUANTILES(upload_med_speed, 1000)[OFFSET(500)] as med_up_speed,
tract
FROM
`oti_usob.Aggregated_MLab_UL_state_house_new`

GROUP BY
day, tract"

query_senate<-"#standardSQL

SELECT day,
APPROX_QUANTILES(med_rtt, 1000)[OFFSET(500)] as med_rtt,
APPROX_QUANTILES(med_speed, 1000)[OFFSET(500)] as med_speed,
APPROX_QUANTILES(upload_med_speed, 1000)[OFFSET(500)] as med_up_speed,
SUM(count_ip) as tract_test_counts,
client_lat, client_lon, tract
FROM
`oti_usob.Aggregated_MLab_DL_state_senate_new`


GROUP BY
client_lat, client_lon, day, tract"

query_senate_up<-"#standardSQL

SELECT day,
APPROX_QUANTILES(upload_med_speed, 1000)[OFFSET(500)] as med_up_speed,
senate_tract
FROM
`oti_usob.Aggregated_MLab_UL_state_senate_new`

GROUP BY
day, senate_tract"

query_477<-"#standardSQL
SELECT
IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
COUNT(DISTINCT DBA_Name) AS num_con_prov,
APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
`TABLE`
WHERE
Consumer = '1'
GROUP BY
FIPS_tract"

query_477_prov<-"#standardSQL
SELECT
APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,
1000)[ OFFSET (500)] AS med_dl,
APPROX_QUANTILES(Max_Advertised_Upstream_Speed__mbps_,
1000)[ OFFSET (500)] AS med_ul,
Provider_name,
SUBSTR(IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)),1,5) AS FIPS_tract
FROM
`TABLE`
WHERE
Consumer = '1'
GROUP BY
Provider_name,
FIPS_tract
"

tract_count_query <- "#standardSQL
SELECT DATE_TRUNC(partition_date, MONTH) AS day, COUNT(connection_spec.client_ip) as count_ip, tract

FROM 
`oti_usob.ndt_spatial_census_tract_new`

GROUP BY 

tract, day"

senate_count_query <- "#standardSQL
SELECT DATE_TRUNC(partition_date, MONTH) AS day, COUNT(connection_spec.client_ip) as count_ip, tract

FROM 
`oti_usob.ndt_spatial_state_senate_new`

GROUP BY 

tract, day"

house_count_query <- "#standardSQL
SELECT DATE_TRUNC(partition_date, MONTH) AS day, COUNT(connection_spec.client_ip) as count_ip, tract

FROM 
`oti_usob.ndt_spatial_state_house_new`

GROUP BY 

tract, day"

#####
#I/O#
#####

todo_copies <- load_time_chunks(query_agg_ip)
todo_copies_up <- load_time_chunks(query_agg_up)
tract_count_count<-query_exec(tract_count_query, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D<-left_join(todo_copies, tract_count_count, by = c("day", "tract"))

D_477_2018_dec_prov <- load_477_data(query_477_prov, "oti_usob.477_dec_2018")
D_477_2017_jun_prov <- load_477_data(query_477_prov, "oti_usob.477_jun_2017")
D_477_2016_dec_prov <- load_477_data(query_477_prov, "oti_usob.477_dec_2016")
D_477_2016_jun_prov <- load_477_data(query_477_prov, "oti_usob.477_jun_2016")
D_477_2015_dec_prov <- load_477_data(query_477_prov, "oti_usob.477_dec_2015")
D_477_2015_jun_prov <- load_477_data(query_477_prov, "oti_usob.477_jun_2015")
D_477_2014_dec_prov <- load_477_data(query_477_prov, "oti_usob.477_dec_2014")

D_state_house <- load_time_chunks(query_house)
D_state_house_up <- load_time_chunks(query_house_up)
D_state_house_90q <- load_time_chunks(query_house_90q)
house_count<-query_exec(house_count_query, project = project, use_legacy_sql=FALSE, max_pages = Inf)
names(D_state_house)[7]<-"GEOID"
names(house_count)[3]<-"GEOID"

D_state_senate <- load_time_chunks(query_senate)
D_state_senate_up <- load_time_chunks(query_senate_up)
senate_count<-query_exec(senate_count_query,project = project, use_legacy_sql=FALSE, max_pages = Inf)
names(D_state_senate)[7]<-"GEOID"
names(senate_count)[3]<-"GEOID"

save(D, file="MLab_data_census_tract1")
save(todo_copies_up, file = "MLab_data_census_tract1_up" )
save(D_477_2018_dec_prov, file="D_477_2018_dec_prov")
save(D_477_2017_jun_prov, file="D_477_2017_jun_prov")
save(D_477_2016_dec_prov, file="D_477_2016_dec_prov")
save(D_477_2016_jun_prov, file="D_477_2016_jun_prov")
save(D_477_2015_dec_prov, file="D_477_2015_dec_prov")
save(D_477_2015_jun_prov, file="D_477_2015_jun_prov")
save(D_477_2014_dec_prov, file="D_477_2014_dec_prov")

save(D_state_house, file="MLab_data_state_house_2")
save(D_state_house_up, file="MLab_data_state_house_up")
save(house_count, file="house_counts_2")
save(D_state_senate, file="MLab_data_state_senate_2")
save(D_state_senate_up, file="MLab_data_state_senate_up")
save(senate_count, file="senate_counts_2")
