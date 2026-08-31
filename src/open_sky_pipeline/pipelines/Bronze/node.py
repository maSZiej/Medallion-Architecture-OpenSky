from opensky_api import OpenSkyApi, TokenManager
from pyspark.sql.types import StructType, StructField, StringType, MapType
from pyspark.sql.functions import current_timestamp
from pyspark.sql import DataFrame,SparkSession
from pathlib import Path
from kedro.config import OmegaConfigLoader
from kedro.framework.project import settings

# __file__ to ścieżka do pliku utils.py
# .parents[2] cofa się o 2 poziomy w górę (z src/open_sky_pipeline -> src -> root)


def read_data_from_api()->DataFrame:
    spark = SparkSession.builder.getOrCreate()
    aircraft_df=[]
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

    project_root = Path(__file__).resolve().parents[4]
    conf_path = str(project_root / settings.CONF_SOURCE)
    conf_loader = OmegaConfigLoader(conf_source=conf_path)

    credentials = conf_loader["credentials"]
    credentials = credentials['opensky_api']
    # print(credentials['opensky_api'])
    with OpenSkyApi(client_id=credentials['clientId'],client_secret=credentials['clientSecret']) as api:
        states = api.get_states()

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
    return aircraft_spark_df