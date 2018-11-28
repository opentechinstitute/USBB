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
AVG(count_ip) as tract_test_counts, tract
FROM
`thieme.D_deserts_final_minrtt_state`

GROUP BY
day, tract"

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

query_477_2017_jun <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477_jun_2017`
WHERE
  Consumer = '1'
GROUP BY
  FIPS_tract"

query_477_2016_dec <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477_dec_2016`
WHERE
  Consumer = '1'
GROUP BY
  FIPS_tract"
query_477_2016_jun <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477_jun_2016`
WHERE
  Consumer = '1'
GROUP BY
  FIPS_tract"
query_477_2015_dec <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477_dec_2015`
WHERE
  Consumer = '1'
GROUP BY
  FIPS_tract"
query_477_2015_jun <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477_jun_2015`
WHERE
  Consumer = '1'
GROUP BY
  FIPS_tract"
query_477_2014_dec <-"#standardSQL
SELECT
  IF(CHAR_LENGTH(CAST(Census_Block_FIPS_CODE AS STRING))=14,SUBSTR(CONCAT('0',CAST(Census_Block_FIPS_CODE AS STRING)),1,11),
  SUBSTR(CAST(Census_Block_FIPS_CODE AS STRING),1,11)) AS FIPS_tract,
  COUNT(DISTINCT DBA_Name) AS num_con_prov,
  APPROX_QUANTILES(Max_Advertised_Downstream_Speed__mbps_,1000)[OFFSET(500)] AS med_dl,
  APPROX_QUANTILES( Max_Advertised_Upstream_Speed__mbps_ ,1000)[OFFSET(500)] AS med_ul
FROM
  `thieme.477_dec_2014`
WHERE
  Consumer = '1'
GROUP BY
  FIPS_tract"

#####
#I/O#
#####

todo_copies <- load_time_chunks(query_agg_ip)
D<-todo_copies

D_477_2017_jun <- query_exec(query_477_2017_jun, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D_477_2016_dec <- query_exec(query_477_2016_dec, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D_477_2016_jun <- query_exec(query_477_2016_jun, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D_477_2015_dec <- query_exec(query_477_2015_dec, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D_477_2015_jun <- query_exec(query_477_2015_jun, project = project, use_legacy_sql=FALSE, max_pages = Inf)
D_477_2014_dec <- query_exec(query_477_2014_dec, project = project, use_legacy_sql=FALSE, max_pages = Inf)

D_state_house <- load_time_chunks(query_house)
names(D_state_house)[7]<-"GEOID"

D_state_senate <- load_time_chunks(query_senate)
names(D_state_senate)[7]<-"GEOID"

save(todo_copies, file="MLab_data_census_tract")
save(D_477_2017_jun, file="D_477_2017_jun")
save(D_477_2016_dec, file="D_477_2016_dec")
save(D_477_2016_jun, file="D_477_2016_jun")
save(D_477_2015_dec, file="D_477_2015_dec")
save(D_477_2015_jun, file="D_477_2015_jun")
save(D_477_2014_dec, file="D_477_2014_dec")

save(D_state_house, file="MLab_data_state_house")
save(D_state_senate, file="MLab_data_state_senate")
