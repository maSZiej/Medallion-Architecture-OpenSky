from pyspark.sql.functions import col,create_map, lit,pandas_udf,from_unixtime,when,max
from pyspark.sql.types import FloatType,IntegerType, BooleanType
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.column import Column
from shapely.geometry import Point, Polygon
import pandas as pd
from pathlib import Path
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project
from delta.tables import DeltaTable


@pandas_udf("boolean")
def check_point_in_polygon(long: pd.Series, lat: pd.Series,poland_polygon: Polygon) -> pd.Series:
    return pd.Series(
        [
            poland_polygon.contains(Point(lon, la))
            for lon, la in zip(long, lat)
        ]
    )
def check_rows_count(df:DataFrame)-> int:
    cols=df.columns
    cols.remove("ingestion_timestamp")
    count=df.select(cols).distinct().count()
    return count

def enrich_dataframe(
    df:DataFrame,
    poland_polygon:Polygon,
    float_list:list,
    int_list:list,
    bool_list:list,
    aircraft_map:Column,
    position_source_map:Column
    )->DataFrame:
    
    df=df \
    .withColumns({col: df[col].cast(FloatType()) for col in float_list}) \
    .withColumns({col: df[col].cast(IntegerType()) for col in int_list}) \
    .withColumns({col: df[col].cast(BooleanType()) for col in bool_list})
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
        check_point_in_polygon(col("longitude"), col("latitude"),poland_polygon)
        ))
    df=df.na.drop(subset=["icao24", "callsign"])
    return df
    
def silver_node(Bronze_Layer,Silver_hist):
    # spark = SparkSession.builder.getOrCreate()
    PROJECT_DIR = Path(__file__).resolve().parents[4]
    bootstrap_project(PROJECT_DIR)
    with KedroSession.create(PROJECT_DIR) as session:
        context = session.load_context()
        catalog = context.catalog
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
        20: "Line Obstacle"}
    position_source_dict = {
        0: "ADS-B",
        1: "ASTERIX",
        2: "MLAT",
        3: "FLARM"}
    aircraft_map = create_map(
        *[item for kv in aircraft_dict.items() for item in (lit(kv[0]), lit(kv[1]))]
        )
    position_source_map = create_map(
        *[item for kv in position_source_dict.items() for item in (lit(kv[0]), lit(kv[1]))]
        )
    poland_polygon = Polygon(
        Poland_Polygon["features"][0]["geometry"]["coordinates"][0]
        )
    ################################################################
    # ACTIONS
    df=Bronze_Layer
    max_ingestion=df.select(max('ingestion_timestamp')).first()[0]
    df=df.where(col('ingestion_timestamp')==max_ingestion)
    count_before_enrichment=check_rows_count(df)
    df=enrich_dataframe(df=df,
                        poland_polygon=poland_polygon,
                        float_list=float_list,
                        int_list=int_list,
                        bool_list=bool_list,
                        aircraft_map=aircraft_map,
                        position_source_map=position_source_map
                        )
    count_after_enrichment=check_rows_count(df)
    print(count_before_enrichment==count_after_enrichment)
    return df

def silver_node_hist(*args, **kwargs):
    spark = SparkSession.builder.getOrCreate()
    if not DeltaTable.isDeltaTable(spark, "s3a://meddalion/silver/aircraft"):
        
        return True 
    else:
        df3_hist = spark.read.format("delta").load("s3a://meddalion/silver/aircraft")
        df3_hist.write \
            .format("delta") \
            .mode("append") \
            .save("s3a://meddalion/silver/aircraft_hist")
    return df3_hist