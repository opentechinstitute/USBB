#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#
#Python dataflow code for the United States of Broadband Mapbox map#
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~#

#########################
#Load libraries and data#
#########################

import apache_beam as beam
import math
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam import pvalue

# BigQuery doesn't allow for nested arrays but that's the easiest way to store sets of shape polygons.
# Instead, the R code in the pipeline that should be run before this file outputs an array of strings where each string is an array defining a polygon that has been stringified.
# This function takes that array of strings and turns each string back
# into an actual array.


class to_geom_shape(beam.DoFn):

    def process(self, element):
        import apache_beam as beam
        """
        Returns a list of tuples containing country and duration
        """
        polys = element['Values']
        # this changes between tract and district. tract is for census tracts,
        # district is for legislative distrcts. The right thing to do is
        # probably standerdize
        tract = element['district']
        # If you want to run this code for just one state, you can remove this.
        state = element['state']
        bool_poly = [None] * len(polys)
        for i in range(0, len(polys)):
            pair = polys[i][1:-1].split(",")
            for j in range(0, len(pair)):
                try:
                    pair[j] = float(pair[j])
                except ValueError:
                    pair = [0, 0]
                    break
            bool_poly[i] = pair
        return [(tract, bool_poly, state)]
        # return [(tract, bool_poly)]  for single state

# This function does the actual spatial join. It takes in a set of locations as its primary input and a set of polygons as its "side input." That only matters because in Dataflow, the  main input
# gets split up and parallelized over while the side input is reproduced
# in full at each node. It makes more sense here to reproduce the polygons
# than the NDT locations.


class check_in_tract_side_mpt(beam.DoFn):

    def process(self, element, polys):

        import matplotlib.path as mpltPath
        import math
        """
        Returns a list of tuples containing country and duration
        """
        lat = element['latitude']
        lon = element['longitude']
        state = str(element['state'])  # rem for single state

        tract = [polys[i][0] for i in range(len(polys))]
        poly_list = [polys[i][1] for i in range(len(polys))]
        state_list = [str(polys[i][2]).strip() for i in range(len(polys))]

        x = float(lon)
        y = float(lat)
        n_list = len(poly_list)
        for j in range(0, n_list):
            poly = poly_list[j]
            poly = [[float(poly[i][0]), float(poly[i][1])]
                    for i in range(len(poly))]
            lens = [len(pair) == 2 for pair in poly]
            inds_l = [i for i, x in enumerate(lens) if x]
            poly_list_pairs = [poly[i] for i in inds_l]
            l0 = [item[0] for item in poly_list_pairs]
            l1 = [item[1] for item in poly_list_pairs]
            center_p = [sum(l0) / float(len(l0)), sum(l1) / float(len(l1))]
            origin_poly = [map(float.__sub__, poly_list_pairs[i], center_p)
                           for i in range(len(poly_list_pairs))]
            inds_asd = [math.atan2(origin_poly[i][0], origin_poly[i][1])
                        for i in range(len(origin_poly))]
            sort_list = sorted(range(len(inds_asd)), key=inds_asd.__getitem__)
            sort_poly = [origin_poly[sort_list[i]]
                         for i in range(len(sort_list))]
            poly_list_fin = [map(float.__add__, sort_poly[i], center_p)
                             for i in range(len(sort_poly))]
            n = len(poly_list_fin)
            inside = False
            path = mpltPath.Path(poly_list_fin)
            inside = path.contains_points([[lon, lat]])[0]
            if inside == True:
                results = [(str(lon), str(lat), tract[
                            j], state, state_list[j])]
                # results=[(str(lon), str(lat),tract[j])] for single state
                return(results)

########################
#Create BigQuery Schema#
########################
table_schema = bigquery.TableSchema()
lat_schema = bigquery.TableFieldSchema()
lat_schema.name = 'lat'
lat_schema.type = 'STRING'
lat_schema.mode = 'NULLABLE'
table_schema.fields.append(lat_schema)

long_schema = bigquery.TableSchema()
long_schema = bigquery.TableFieldSchema()
long_schema.name = 'long'
long_schema.type = 'STRING'
long_schema.mode = 'NULLABLE'
table_schema.fields.append(long_schema)

# A nested field
name_schema = bigquery.TableFieldSchema()
name_schema.name = 'tract'
name_schema.type = 'STRING'
name_schema.mode = 'NULLABLE'
table_schema.fields.append(name_schema)

# A nested field
state_schema = bigquery.TableFieldSchema()
state_schema.name = 'state'
state_schema.type = 'STRING'
state_schema.mode = 'NULLABLE'
table_schema.fields.append(state_schema)

# A nested field
state_schema_a = bigquery.TableFieldSchema()
state_schema_a.name = 'state_list'
state_schema_a.type = 'STRING'
state_schema_a.mode = 'NULLABLE'
table_schema.fields.append(state_schema_a)

######################
#Set Dataflow Options#
######################
options = {'project': 'mlab-sandbox',
           'runner': 'DataflowRunner',
           'staging_location': 'gs://oti-usob/staging', #Set this to the staging location you set up inside GCP when initializing Dataflow. 
                                                   #Google's documentation here: https://cloud.google.com/dataflow/docs/guides/specifying-exec-params

           'temp_location': 'gs://oti-usob/temp', #Set this to the temp location you set up inside GCP when initializing Dataflow.

           'setup_file': 'setup.py',  #Set this to the location of the local file setup.py. This is crucial. The Dataflow nodes running python don't have all 
                                              #of the packages needed to run this code and this file tells them to get them. Nothing works without this. 
           'workerCacheSizeMb': 400, #These numbers need to be set by the user in accordance with their budget. These numbers helped the code process many entries very quickly
                                     #but I was lucky to have free access to Google resources when writing this code. These numbers might be very expensive (in human $) otherwise
           'num_workers': 600}

pipeline_options = beam.pipeline.PipelineOptions(flags=[], **options)
pipeline = beam.Pipeline(options=pipeline_options)

###############
#Dataflow Code#
###############

#Dataflow code runs by first specifying a "pipeline." Basically, a pipeline is a promise that certain code will be run in a particular order once you call ".run()"
#Before that, everything is just specfying the promise. 

#All of this code could be condensed but splitting it out and naming it this way is useful when reading Dataflow graphs to understand where there are bugs. I've found this 
#organization breaks the pipeline into its essential steps. tract_shapes imports the array of stringified polygons and converts them into a set of nested arrays representing 
#polygons.
tract_shapes = (
    pipeline | "read tract shapes" >> beam.io.Read(
        beam.io.BigQuerySource(table='pre_dataflow_polygon_house', dataset='oti_usob')) #dataset should be the name of the BigQuery dataset that contains the table of stringified 
                                                                                 #polygons. Table should be the actual table.
    | "turn into polygon" >> beam.ParDo(to_geom_shape())
)

#(tract_shapes|'WriteOutputtracts' >> beam.io.WriteToText('gs://thieme-us-query/477/tracts')) #This is a useful debugging line. If something is going wrong, it's useful to write
                                                                                              #out the nested array of polygons to see if the error is there. The argument to
                                                                                              #WriteToText in my case is a Google Bucket.

#NDT_MO is the BigQuery data of locations to be spatially joined.                                                                             
NDT = pipeline | "read ndt data" >> beam.io.Read(
    beam.io.BigQuerySource(table='US_loc', dataset='oti_usob')) #dataset should be the name of the BigQuery dataset that contains the locations to be joined. 
                                                                                 #Table should be the actual table.

#NDT_shuff seems odd because it doesn't "actually" do anything. It adds a useless key and then flattens the key away. This is used because Dataflow has some weird quirks in 
#how it parallelizes calculations. It's apparerently very smart in how it groups calculations together to save time but in this case, it's a little too smart. A quirk of 
#Dataflow is that if you add a key and get rid of it, it won't try to group calculations. In this case, that means doing this allows us to parallelize how we want.                                                                                 
NDT_shuff = (NDT
                | "add keys" >> beam.Map(lambda x: (x, 1))
                | "group by key" >> beam.GroupByKey()
                | "remove keys" >> beam.FlatMap(lambda x: (x[0] for v in x[1]))
                )
#res produces the spatially joined data we're here for. It takes the result of NDT_shuff (a "dataset" of internet speed test locations as the primary input and 
#the set of polygons as the side input). It runs the functions defined in the library to join those and then creates a dictionary of the new data. That then gets written out to
#a BigQuery table.
res = (NDT_shuff
       | "spatial join" >> beam.ParDo(check_in_tract_side_mpt(), beam.pvalue.AsList(tract_shapes))
       | "convert to dictionary" >> beam.Map(lambda elem: dict(lat=elem[0], long=elem[1], tract=elem[2], state=elem[3], state_list=elem[4]))
       #|'WriteOutputgeom' >> beam.io.WriteToText('gs://thieme-us-query/477/tract_connect_n2') #This is a useful debugging line. If something is going wrong, it's useful to write
                                                                                              #out the nested array of polygons to see if the error is there. The argument to
                                                                                              #WriteToText in my case is a Google Bucket. In partiucular this line is useful
                                                                                              #for debugging writing to BigQuery issues. BigQuery's schema is very particular.
       | 'write to BQ' >> beam.io.WriteToBigQuery(table="dataflow_output_state_house", dataset="oti_usob", schema=table_schema)
       )

pipeline.run()
