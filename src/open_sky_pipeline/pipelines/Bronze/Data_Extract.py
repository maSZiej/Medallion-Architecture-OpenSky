from open_sky_pipeline.Connect import get_spark
from opensky_api import OpenSkyApi, TokenManager
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import current_timestamp

aircraft_df=[]
lista={}
jdbc_url = "jdbc:postgresql://localhost:5432/OpenSky"
target_table = 'aircraft'
schema = StructType([
StructField("icao24", StringType(), True),
StructField("callsign",  StringType(), True),
StructField("origin_country",  StringType(), True),
StructField("time_position",  StringType(), True),
StructField("last_contact",  StringType(), True),
StructField("longitude",  StringType(), True),
StructField("latitude",  StringType(), True),
StructField("geo_altitude",  StringType(), True),
StructField("on_ground",  StringType(), True),
StructField("velocity",  StringType(), True),
StructField("true_track",  StringType(), True),
StructField("vertical_rate",  StringType(), True),
StructField("sensors",  StringType(), True),
StructField("baro_altitude",  StringType(), True),
StructField("squawk",  StringType(), True),
StructField("spi",  StringType(), True),
StructField("position_source",  StringType(), True),
StructField("category",  StringType(), True)
])

spark=get_spark()

with OpenSkyApi(token_manager=TokenManager.from_json_file("credentials.json")) as api:
    states = api.get_states()
states_df = states

for state in states.states:
    aircraft_df.extend(
    [(
    state.icao24,
    state.callsign,
    state.origin_country,
    state.time_position,
    state.last_contact,
    state.longitude,
    state.latitude,
    state.geo_altitude,
    state.on_ground,
    state.velocity,
    state.true_track,
    state.vertical_rate,
    state.sensors,
    state.baro_altitude,
    state.squawk,
    state.spi,
    state.position_source,
    state.category
    )])
    
aircraft_spark_df = spark.createDataFrame(aircraft_df, schema=schema)

aircraft_spark_df = aircraft_spark_df.withColumn("ingestion_timestamp", current_timestamp())

try:
    aircraft_spark_df.write \
        .format("delta") \
        .mode("append") \
        .save(f"s3a://meddalion/bronze/{target_table}")
except Exception as e:
    print(e)