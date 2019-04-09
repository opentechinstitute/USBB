from google.cloud import bigquery

from string import Template
import time

ndt_join = Template (
    'WITH USA AS (SELECT * '
    'FROM `measurement-lab.release.ndt_all` '
    'WHERE '
    'connection_spec.server_geolocation.country_name = "United States") '
    'SELECT * '
    'FROM USA LEFT JOIN `oti_usob.post_dataflow_${geom}` AS dataflow '
    'ON SUBSTR(CAST(USA.connection_spec.client_geolocation.latitude AS STRING), 0,9) '
    '= SUBSTR(CAST(dataflow.long AS STRING), 0,9) '
    'AND SUBSTR(CAST(USA.connection_spec.client_geolocation.longitude AS STRING), 0,10) '
    '= SUBSTR(CAST(dataflow.lat AS STRING), 0,10)' )

formatting_UL_join = Template (
    'WITH '
    'D_ip_speed AS ( '
    'SELECT '
        'DATE_TRUNC(CAST(DATE(log_time) AS DATE), MONTH) AS day, '
        'COUNT(connection_spec.client_ip) AS count_ip, '
        'APPROX_QUANTILES(8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsReceived, web100_log_entry.snap.Duration),1000)'
        '[OFFSET (500)] AS upload_med_speed, '
        'APPROX_QUANTILES(SAFE_DIVIDE(web100_log_entry.snap.SumRTT, web100_log_entry.snap.CountRTT), 1000)'
        '[OFFSET(500)] AS med_rtt, '
        'connection_spec.server_geolocation.latitude AS serv_lat1, '
        'connection_spec.server_geolocation.longitude AS serv_lon1, '
        'connection_spec.client_geolocation.latitude AS client_lat1, '
        'connection_spec.client_geolocation.longitude AS client_lon1 '
    'FROM '
        '`oti_usob.ndt_spatial_${geom}`  '
    'WHERE connection_spec.data_direction = 0 '
        'AND web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 600000000 '
        'AND (web100_log_entry.snap.State = 1 '
            'OR (web100_log_entry.snap.State >= 5 '
            'AND web100_log_entry.snap.State <= 11))'
    'GROUP BY '
        'day, '
        'serv_lat1, '
        'serv_lon1, '
        'client_lat1, '
        'client_lon1 ), '
    'D_ip_tract AS ( '
    'SELECT '
        'DISTINCT connection_spec.client_ip AS ip1, '
        'tract, '
        'connection_spec.server_geolocation.latitude AS serv_lat, '
        'connection_spec.server_geolocation.longitude AS serv_lon, '
        'connection_spec.client_geolocation.latitude AS client_lat, '
        'connection_spec.client_geolocation.longitude AS client_lon '
    'FROM '
        '`oti_usob.ndt_spatial_${geom}` '
    'WHERE '
        'connection_spec.data_direction = 0 '
        'AND web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 600000000 '
        'AND (web100_log_entry.snap.State = 1 '
            'OR (web100_log_entry.snap.State >= 5 '
            'AND web100_log_entry.snap.State <= 11)) )'
    'SELECT '
    'DISTINCT * '
    'FROM '
    'D_ip_speed '
    'LEFT JOIN '
    'D_ip_tract '
    'ON '
    'D_ip_speed.client_lat1=D_ip_tract.client_lat '
    'AND D_ip_speed.client_lon1=D_ip_tract.client_lon '
    'AND D_ip_speed.serv_lat1=D_ip_tract.serv_lat '
    'AND D_ip_speed.serv_lon1=D_ip_tract.serv_lon' )

formatting_DL_join = Template (
    'WITH '
    'D_ip_speed AS ( '
    'SELECT '
        'DATE_TRUNC(CAST(DATE(log_time) AS DATE), MONTH) AS day, '
        'COUNT(connection_spec.client_ip) AS count_ip, '
        'APPROX_QUANTILES(8 * SAFE_DIVIDE(web100_log_entry.snap.HCThruOctetsAcked, '
        '(web100_log_entry.snap.SndLimTimeRwin + '
        'web100_log_entry.snap.SndLimTimeCwnd + '
        'web100_log_entry.snap.SndLimTimeSnd)),1000)[OFFSET (500)] AS download_med_speed, '
        'APPROX_QUANTILES(SAFE_DIVIDE(web100_log_entry.snap.SumRTT, web100_log_entry.snap.CountRTT), 1000)'
        '[OFFSET(500)] AS med_rtt, '
        'connection_spec.server_geolocation.latitude AS serv_lat1, '
        'connection_spec.server_geolocation.longitude AS serv_lon1, '
        'connection_spec.client_geolocation.latitude AS client_lat1, '
        'connection_spec.client_geolocation.longitude AS client_lon1 '
    'FROM '
        '`oti_usob.ndt_spatial_${geom}`  '
    'WHERE connection_spec.data_direction = 1 '
        'AND web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 600000000 '
        'AND (web100_log_entry.snap.State = 1 '
            'OR (web100_log_entry.snap.State >= 5 '
            'AND web100_log_entry.snap.State <= 11))'
    'GROUP BY '
        'day, '
        'serv_lat1, '
        'serv_lon1, '
        'client_lat1, '
        'client_lon1 ), '
    'D_ip_tract AS ( '
    'SELECT '
        'DISTINCT connection_spec.client_ip AS ip1, '
        'tract, '
        'connection_spec.server_geolocation.latitude AS serv_lat, '
        'connection_spec.server_geolocation.longitude AS serv_lon, '
        'connection_spec.client_geolocation.latitude AS client_lat, '
        'connection_spec.client_geolocation.longitude AS client_lon '
    'FROM '
        '`oti_usob.ndt_spatial_${geom}` '
    'WHERE '
        'connection_spec.data_direction = 1 '
        'AND web100_log_entry.snap.HCThruOctetsReceived >= 8192 '
        'AND web100_log_entry.snap.Duration >= 9000000 '
        'AND web100_log_entry.snap.Duration < 600000000 '
        'AND (web100_log_entry.snap.State = 1 '
            'OR (web100_log_entry.snap.State >= 5 '
            'AND web100_log_entry.snap.State <= 11)) )'
    'SELECT '
    'DISTINCT * '
    'FROM '
    'D_ip_speed '
    'LEFT JOIN '
    'D_ip_tract '
    'ON '
    'D_ip_speed.client_lat1=D_ip_tract.client_lat '
    'AND D_ip_speed.client_lon1=D_ip_tract.client_lon '
    'AND D_ip_speed.serv_lat1=D_ip_tract.serv_lat '
    'AND D_ip_speed.serv_lon1=D_ip_tract.serv_lon' )

client = bigquery.Client(project = 'mlab-sandbox')

geometries = ["tract", "house", "senate"]

for g in geometries: 
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset('oti_usob').table(Template ('ndt_spatial_${geom}').safe_substitute(geom = g))
    job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_TRUNCATE'
    print("Starting ndt job: " + g)
    ndt_job = client.query(
        job_id = "usbb_pipeline_ndt_" + g + "_" + str(int(time.time())),
        query = ndt_join.safe_substitute(geom = g),
        job_config = job_config
    )
    print(ndt_job.job_id)

while not ndt_job.done():
    time.sleep(10)

for g in geometries:
    job_config = bigquery.QueryJobConfig()
    table_ref = client.dataset('oti_usob').table(Template ('Aggregated_MLab_UL_${geom}').safe_substitute(geom = g))
    job_config.destination = table_ref
    job_config.write_disposition = 'WRITE_TRUNCATE'
    print("Starting format job: " + g + ", UL")
    format_job = client.query(
        job_id = "usbb_pipeline_formatting_Agg" + g + "UL" + str(int(time.time())),
        query = formatting_UL_join.safe_substitute(geom = g),
        job_config = job_config
    )
    job_config2 = bigquery.QueryJobConfig()
    table_ref2 = client.dataset('oti_usob').table(Template ('Aggregated_MLab_DL_${geom}').safe_substitute(geom = g))
    job_config2.destination = table_ref2
    job_config2.write_disposition = 'WRITE_TRUNCATE'
    print("Starting format job: " + g + ", DL")
    format_job = client.query(
        job_id = "usbb_pipeline_formatting_Agg" + g + "DL" + str(int(time.time())),
        query = formatting_DL_join.safe_substitute(geom = g),
        job_config = job_config2
    )
    

while not format_job.done():
    time.sleep(10)

print("All queries finished.")