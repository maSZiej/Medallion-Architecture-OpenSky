from open_sky_pipeline.Connect import get_spark
from pyspark.sql.functions import col,create_map, lit,pandas_udf,from_unixtime,when,max
from pyspark.sql.types import FloatType, DateType, StringType,IntegerType, BooleanType
from pyspark.sql import DataFrame,SparkSession
from shapely.geometry import Point, Polygon
import json
import pandas as pd
from kedro.io import DataCatalog
from pathlib import Path
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

# Inicjalizacja kontekstu projektu Kedro

def silver_node(Bronze_Layer,Silver_hist):
    spark = SparkSession.builder.getOrCreate()
    
    @pandas_udf("boolean")
    def _check_point_in_polygon(long: pd.Series, lat: pd.Series) -> pd.Series:
        return pd.Series(
            [
                poland_polygon.contains(Point(lon, la))
                for lon, la in zip(long, lat)
            ]
        )
    def _check_rows_count(df:DataFrame)-> int:
        cols=df.columns
        cols.remove("ingestion_timestamp")
        count=df.select(cols).distinct().count()
        return count
    #############
    # VARIABLES
    PROJECT_DIR = Path(__file__).resolve().parents[4]
    bootstrap_project(PROJECT_DIR)

    with KedroSession.create(PROJECT_DIR) as session:
        context = session.load_context()
        catalog = context.catalog
        
        # Wczytanie samych DANYCH z katalogu
        Poland_Polygon = catalog.load("Poland_polygon")
        


    int_list=["time_position","last_contact","position_source","category"]
    float_list=["longitude","latitude","geo_altitude","velocity","true_track","vertical_rate","baro_altitude"]
    bool_list=["on_ground","spi"]

    aircraft_dict = {
        0: "No Info",
        1: "Light",
        2: "Small",
        3: "Medium",
        4: "Large",
        5: "High Vortex Large",
        6: "Heavy",
        7: "High Performance",
        8: "Rotorcraft",
        9: "Glider / Sailplane",
        10: "Lighter than Air",
        11: "Parachutist / Skydiver",
        12: "Ultralight / Hang Glider / Paraglider",
        13: "Reserved / Unassigned",
        14: "Unmanned Aerial Vehicle (UAV)",
        15: "Space / Trans-atmospheric Vehicle",
        16: "Surface Vehicle - Emergency Vehicle",
        17: "Surface Vehicle – Service Vehicle",
        18: "Point Obstacle",
        19: "Cluster Obstacle",
        20: "Line Obstacle"
    }

    position_source_dict = {
        0: "ADS-B",
        1: "ASTERIX",
        2: "MLAT",
        3: "FLARM"
    }


    aircraft_map = create_map(
        *[item for kv in aircraft_dict.items() for item in (lit(kv[0]), lit(kv[1]))]
    )
    position_source_map = create_map(
        *[item for kv in position_source_dict.items() for item in (lit(kv[0]), lit(kv[1]))]
    )

    # with open("poland.json", "r") as f:
    #     Poland_Polygon = json.load(f)
        
    poland_polygon = Polygon(
        Poland_Polygon["features"][0]["geometry"]["coordinates"][0]
    )
    ################################################################
    # ACTIONS

    # df3 = spark.read.format("delta").load("s3a://meddalion/bronze/aircraft")
    df3=Bronze_Layer
    max_ingestion=df3.select(max('ingestion_timestamp')).first()[0]
    df3=df3.where(col('ingestion_timestamp')==max_ingestion)


    df=df3 \
        .withColumns({col: df3[col].cast(FloatType()) for col in float_list}) \
        .withColumns({col: df3[col].cast(IntegerType()) for col in int_list}) \
        .withColumns({col: df3[col].cast(BooleanType()) for col in bool_list})

    count_before_enrichment=_check_rows_count(df)
    df=df.na.drop(subset=["icao24", "callsign"])
    df=(df   
        
        #Enrichment
        .withColumn(
            "altitude_diff", 
            col("geo_altitude") - col("baro_altitude")
            )
        .withColumn(
            "last_contact_h",
            from_unixtime(col("last_contact"))
            )
        .withColumn(
            "time_position_h",
            from_unixtime(col("time_position"))
            )
        .withColumn(
            "vertical_category",
            when(col("vertical_rate") > 0, "Climbing")
            .otherwise(when(col("vertical_rate")==0,"Constant Altitude")
            .otherwise("Descending"))
            )
        .withColumn(
            "aircraft_category",
            aircraft_map.getItem(col("category"))
            )
        .withColumn(
            "position_source_name",
            position_source_map.getItem(col("position_source"))
            )
        .withColumn(
            "isPoland",
            _check_point_in_polygon(col("longitude"), col("latitude"))
            )
    )

    count_after_enrichment=_check_rows_count(df)


    print(count_before_enrichment==count_after_enrichment)
    # try:
    #     df.write \
    #         .format("delta") \
    #         .mode("overwrite") \
    #         .save("s3a://meddalion/silver/aircraft")
    # except Exception as e:
    #     print(e)
    return df

        
    # df.show()
def silver_node_hist(*args, **kwargs):
    spark = SparkSession.builder.getOrCreate()
    df3_hist = spark.read.format("delta").load("s3a://meddalion/silver/aircraft")
    df3_hist.write \
        .format("delta") \
        .mode("append") \
        .save("s3a://meddalion/silver/aircraft_hist")
    print('Hi')
    return df3_hist