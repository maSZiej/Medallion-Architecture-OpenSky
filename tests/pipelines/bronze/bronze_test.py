import pytest
from open_sky_pipeline.pipelines.bronze.node import define_schema,map_data,get_timestamp
from pyspark.sql.types import StructType
# from open_sky_pipeline.tests.conftest import get_spark
from pyspark.sql import DataFrame

def test_schema():
    schema=define_schema()
    columns=[field.name for field in schema.fields]
    assert columns == [
        'icao24',
        'callsign',
        'origin_country',
        'time_position',
        'last_contact',
        'longitude',
        'latitude',
        'geo_altitude',
        'on_ground',
        'velocity',
        'true_track',
        'vertical_rate',
        'sensors',
        'baro_altitude',
        'squawk',
        'spi',
        'position_source',
        'category'
        ]
def test_get_timestamp(get_spark):
    result=get_timestamp(get_spark,[],StructType())
    assert isinstance(result,DataFrame)
    assert "ingestion_timestamp"  in result.columns


def test_map_data(sample_states):
    list_states=map_data(states=sample_states)
    assert len(list_states[0])==18
    assert len(list_states)==3