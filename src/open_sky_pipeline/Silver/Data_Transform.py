from open_sky_pipeline.Connect import get_spark
from pyspark.sql.functions import col,create_map, lit,pandas_udf,from_unixtime,when
from shapely.geometry import Point, Polygon
import json
import pandas as pd

spark=get_spark()

@pandas_udf("boolean")
def check_point_in_polygon(long: pd.Series, lat: pd.Series) -> pd.Series:
    return pd.Series(
        [
            poland_polygon.contains(Point(lon, la))
            for lon, la in zip(long, lat)
        ]
    )

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

with open("poland.json", "r") as f:
    Poland_Polygon = json.load(f)
    
poland_polygon = Polygon(
    Poland_Polygon["features"][0]["geometry"]["coordinates"][0]
)