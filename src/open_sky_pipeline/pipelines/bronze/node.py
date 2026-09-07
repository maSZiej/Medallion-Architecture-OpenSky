from pathlib import Path

from kedro.config import OmegaConfigLoader
from kedro.framework.project import settings
from opensky_api import OpenSkyApi, OpenSkyStates
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def define_schema() -> StructType:
    schema = StructType(
        [
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", StringType(), True),
            StructField("last_contact", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("geo_altitude", StringType(), True),
            StructField("on_ground", StringType(), True),
            StructField("velocity", StringType(), True),
            StructField("true_track", StringType(), True),
            StructField("vertical_rate", StringType(), True),
            StructField("sensors", StringType(), True),
            StructField("baro_altitude", StringType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", StringType(), True),
            StructField("position_source", StringType(), True),
            StructField("category", StringType(), True),
        ]
    )
    return schema


def connect_Sky_Api() -> list:
    project_root = Path(__file__).resolve().parents[4]
    conf_path = str(project_root / settings.CONF_SOURCE)
    conf_loader = OmegaConfigLoader(conf_source=conf_path)

    credentials = conf_loader["credentials"]
    credentials = credentials["opensky_api"]
    with OpenSkyApi(
        client_id=credentials["clientId"], client_secret=credentials["clientSecret"]
    ) as api:
        states = api.get_states()
    return states


def map_data(states: OpenSkyStates) -> list:
    aircraft_df = []
    for state in states.states:
        aircraft_df.append(
            (
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
                state.category,
            )
        )
    return aircraft_df


def get_timestamp(
    spark: SparkSession, aircraft_df: list, schema: StructType
) -> DataFrame:
    aircraft_spark_df = spark.createDataFrame(aircraft_df, schema=schema)
    aircraft_spark_df = aircraft_spark_df.withColumn(
        "ingestion_timestamp", current_timestamp()
    )
    return aircraft_spark_df


def format_data(
    df: DataFrame, float_list: list, int_list: list, bool_list: list
) -> DataFrame:
    df = (
        df.withColumns({col: df[col].cast(FloatType()) for col in float_list})
        .withColumns({col: df[col].cast(IntegerType()) for col in int_list})
        .withColumns({col: df[col].cast(BooleanType()) for col in bool_list})
    )
    return df


def read_data_from_api() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    int_list = ["time_position", "last_contact", "position_source", "category"]
    float_list = [
        "longitude",
        "latitude",
        "geo_altitude",
        "velocity",
        "true_track",
        "vertical_rate",
        "baro_altitude",
    ]
    bool_list = ["on_ground", "spi"]
    schema = define_schema()
    states = connect_Sky_Api()
    aircraft_df = map_data(states)
    aircraft_spark_df = get_timestamp(spark, aircraft_df, schema)
    aircraft_spark_df = format_data(aircraft_spark_df, float_list, int_list, bool_list)
    return aircraft_spark_df
