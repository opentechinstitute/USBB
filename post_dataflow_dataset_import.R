#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#|Script to import spatially-joined data from BigQuery|#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#########
#Library#
#########

library(bigrquery)
project <- "mlab-sandbox"

##################
#BigQuery queries#
##################

query_agg_ip<-"#standardSQL

SELECT day, APPROX_QUANTILES(min_rtt, 1000)[OFFSET(500)] as min_rtt,
APPROX_QUANTILES(med_rtt, 1000)[OFFSET(500)] as med_rtt,
APPROX_QUANTILES(med_speed, 1000)[OFFSET(500)] as med_speed,
AVG(count_ip) as tract_test_counts,
client_lat, client_lon, tract
FROM
`thieme.D_deserts_final_minrtt_state`

GROUP BY
client_lat, client_lon, day, tract"

query_house<-"#standardSQL

SELECT day,
APPROX_QUANTILES(med_rtt, 1000)[OFFSET(500)] as med_rtt,
APPROX_QUANTILES(med_speed, 1000)[OFFSET(500)] as med_speed,
AVG(count_ip) as tract_test_counts,
client_lat, client_lon, tract
FROM
`thieme.D_joined_state_house`

GROUP BY
client_lat, client_lon, day, tract"

query_senate<-"#standardSQL

SELECT day,
APPROX_QUANTILES(med_rtt, 1000)[OFFSET(500)] as med_rtt,
APPROX_QUANTILES(med_speed, 1000)[OFFSET(500)] as med_speed,
AVG(count_ip) as tract_test_counts,
client_lat, client_lon, tract
FROM
`thieme.D_joined_state_senate`

GROUP BY
client_lat, client_lon, day, tract"

query_477 <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477`
WHERE
  Consumer = 1
GROUP BY
  FIPS_tract"

#####
#I/O#
#####

todo_copies <- query_exec(query_agg_ip, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D<-todo_copies

D_477 <- query_exec(query_477, project = project, use_legacy_sql=FALSE, max_pages = Inf)

D_state_house <- query_exec(query_house, project = project, use_legacy_sql=FALSE, max_pages = Inf)
names(D_state_house)[7]<-"GEOID"

D_state_senate <- query_exec(query_senate, project = project, use_legacy_sql=FALSE, max_pages = Inf)
names(D_state_senate)[7]<-"GEOID"

save(todo_copies, file="MLab_data_census_tract")
save(D_477, file="477_data")
save(D_state_house, file="MLab_data_state_house")
save(D_state_senate, file="MLab_data_state_senate")

