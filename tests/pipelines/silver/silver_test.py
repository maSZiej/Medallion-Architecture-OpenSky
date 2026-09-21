from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from shapely.geometry import Point, Polygon
from open_sky_pipeline.pipelines.silver.node import (
    enrich_dataframe,
    Init_maps
)
from pathlib import Path
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

def test_silver_workflow(sample_df_bronze,sample_df):
    
    PROJECT_DIR = Path(__file__).resolve().parents[3]
    print(f"Project directory: {PROJECT_DIR}")
    bootstrap_project(PROJECT_DIR)
    with KedroSession.create(PROJECT_DIR) as session:
        context = session.load_context()
        catalog = context.catalog
        Poland_Polygon = catalog.load("Poland_polygon")
    poland_polygon = Polygon(
        Poland_Polygon["features"][0]["geometry"]["coordinates"][0]
    )
    
    aircraft_map, position_source_map=Init_maps()
    enriched_df = enrich_dataframe(
        sample_df_bronze, 
        poland_polygon,
        aircraft_map, 
        position_source_map)
    
    assert enriched_df.count()>0
    assert enriched_df.schema ==sample_df.schema
