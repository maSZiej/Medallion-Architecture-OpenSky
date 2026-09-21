from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from open_sky_pipeline.pipelines.gold.node import aggregate_data


def test_aggregate_data(sample_df, sample_df_hist):
    df_fact, df_aircraft_dim, df_all, df_cat = aggregate_data(sample_df, sample_df_hist)
    assert (
        all([df_fact.count(), df_aircraft_dim.count(), df_all.count(), df_cat.count()])
        > 0
    )

    expected_fact_types = StructType(
        [
            StructField("callsign", StringType(), True),
            StructField("last_contact_h", StringType(), True),
            StructField("time_position_h", StringType(), True),
            StructField("longitude", FloatType(), True),
            StructField("latitude", FloatType(), True),
            StructField("geo_altitude", FloatType(), True),
            StructField("baro_altitude", FloatType(), True),
            StructField("altitude_diff", FloatType(), True),
            StructField("true_track", FloatType(), True),
            StructField("velocity", FloatType(), True),
            StructField("vertical_category", StringType(), False),
            StructField("on_ground", BooleanType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
        ]
    )
    expected_dim_types = StructType(
        [
            StructField("icao24", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("aircraft_category", StringType(), True),
        ]
    )
    expected_kpi_types = StructType(
        [
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("all_observation_count", LongType(), False),
            StructField("count_flying", LongType(), True),
            StructField("on_ground_count", LongType(), True),
            StructField("avg_velocity", DoubleType(), True),
            StructField("avg_baro_alt", DoubleType(), True),
        ]
    )
    expected_cat_types = StructType(
        [
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("vertical_category", StringType(), False),
            StructField("avg_baro_alt", DoubleType(), True),
            StructField("avg_velocity", DoubleType(), True),
            StructField("flying_count", LongType(), False),
        ]
    )
    expected_types = [
        expected_fact_types,
        expected_dim_types,
        expected_kpi_types,
        expected_cat_types,
    ]
    frames = [df_fact, df_aircraft_dim, df_all, df_cat]
    for df, exp_type in zip(frames, expected_types):
        assert df.schema == exp_type
