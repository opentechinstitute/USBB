-- Step One: Run the following query for each 

#standardSQL

WITH USA AS (SELECT * 
FROM `measurement-lab.release.ndt_all` 
WHERE 
connection_spec.server_geolocation.country_name = 'United States'
)
SELECT *
FROM USA LEFT JOIN `thieme.dataflow_mapbox_upper_DL` AS dataflow ON SUBSTR(CAST(USA.connection_spec.client_geolocation.latitude AS STRING), 0,9)
= SUBSTR(CAST(dataflow.long AS STRING), 0,9)
AND SUBSTR(CAST(USA.connection_spec.client_geolocation.longitude AS STRING), 0,10) = SUBSTR(CAST(dataflow.lat AS STRING), 0,10)


#standardSQL

WITH D_ip_speed AS ( 
SELECT DATE_TRUNC(partition_date, MONTH) AS day, COUNT(connection_spec.client_ip) as count_ip,

APPROX_QUANTILES(web100_log_entry.snap.SumRTT/web100_log_entry.snap.CountRTT, 1000)[OFFSET(500)] AS med_rtt,

APPROX_QUANTILES(8 *(web100_log_entry.snap.HCThruOctetsAcked /(web100_log_entry.snap.SndLimTimeRwin +
web100_log_entry.snap.SndLimTimeCwnd +web100_log_entry.snap.SndLimTimeSnd)), 1000)[OFFSET(500)] AS med_speed,
connection_spec.server_geolocation.latitude AS serv_lat1, 
connection_spec.server_geolocation.longitude AS serv_lon1, connection_spec.client_geolocation.latitude AS client_lat1, 
connection_spec.client_geolocation.longitude AS client_lon1
FROM
`measurement-lab.release.ndt_downloads`
WHERE 
web100_log_entry.snap.CountRTT > 10 AND
connection_spec.client_geolocation.country_name = 'United States'
AND
connection_spec.data_direction = 1
AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
AND (web100_log_entry.snap.SndLimTimeRwin +
web100_log_entry.snap.SndLimTimeCwnd +
web100_log_entry.snap.SndLimTimeSnd) >= 9000000
AND (web100_log_entry.snap.SndLimTimeRwin +
web100_log_entry.snap.SndLimTimeCwnd +
web100_log_entry.snap.SndLimTimeSnd) < 600000000
AND web100_log_entry.snap.CongSignals > 0
AND (web100_log_entry.snap.State = 1 OR
(web100_log_entry.snap.State >= 5 AND
web100_log_entry.snap.State <= 11))
GROUP BY
day, serv_lat1, serv_lon1,client_lat1, client_lon1

)
, D_ip_tract AS
(

SELECT DISTINCT connection_spec.client_ip AS ip1, tract, connection_spec.server_geolocation.latitude AS serv_lat, 
connection_spec.server_geolocation.longitude AS serv_lon, connection_spec.client_geolocation.latitude AS client_lat, 
connection_spec.client_geolocation.longitude AS client_lon
FROM
`thieme.ndt_spatial_full_new` 
WHERE 
connection_spec.data_direction = 1
AND web100_log_entry.snap.HCThruOctetsAcked >= 8192
AND (web100_log_entry.snap.SndLimTimeRwin +
web100_log_entry.snap.SndLimTimeCwnd +
web100_log_entry.snap.SndLimTimeSnd) >= 9000000
AND (web100_log_entry.snap.SndLimTimeRwin +
web100_log_entry.snap.SndLimTimeCwnd +
web100_log_entry.snap.SndLimTimeSnd) < 600000000
AND web100_log_entry.snap.CongSignals > 0
AND (web100_log_entry.snap.State = 1 OR
(web100_log_entry.snap.State >= 5 AND
web100_log_entry.snap.State <= 11))
)

SELECT DISTINCT * 

FROM 
D_ip_speed LEFT JOIN D_ip_tract ON D_ip_speed.client_lat1=D_ip_tract.client_lat AND
D_ip_speed.client_lon1=D_ip_tract.client_lon AND D_ip_speed.serv_lat1=D_ip_tract.serv_lat AND
D_ip_speed.serv_lon1=D_ip_tract.serv_lon