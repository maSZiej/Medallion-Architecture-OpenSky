import os
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from datetime import datetime

@pytest.fixture
def get_spark():
    PACKAGES = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "org.postgresql:postgresql:42.7.3",
    ]

    project_root = Path(__file__).resolve().parents[1]
    # print(project_root)
    HADOOP_HOME = f"{project_root}/jars/hadoop"
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["PATH"] = (
        os.path.join(HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
    )

    # os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.2"
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName("test")
        .master("local[*]")
        # ---- S3A / MinIO Connection ----
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        # ---- Timeouts & Stability ----
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        # ---- Delta Lake ----
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # ---- Performance ----
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        ### ---- Driver ----
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp/spark")
    )

    spark = configure_spark_with_delta_pip(
        builder, extra_packages=PACKAGES
    ).getOrCreate()

    h_conf = spark.sparkContext._jsc.hadoopConfiguration()
    h_conf.set("fs.s3a.threads.keepalivetime", "60000")
    h_conf.set("fs.s3a.multipart.purge.age", "86400000")
    yield spark
    spark.stop()


@pytest.fixture
def sample_states():
    return SimpleNamespace(
        states=[
            SimpleNamespace(
                icao24="ICAO1",
                callsign="CALL1",
                origin_country="PL",
                time_position=1700000000,
                last_contact=1700000100,
                longitude=19.0,
                latitude=52.0,
                geo_altitude=1000.0,
                on_ground=False,
                velocity=120.0,
                true_track=90.0,
                vertical_rate=5.0,
                sensors=None,
                baro_altitude=950.0,
                squawk="1234",
                spi=False,
                position_source=1,
                category=3,
            ),
            SimpleNamespace(
                icao24="ICAO2",
                callsign="CALL2",
                origin_country="DE",
                time_position=1700000200,
                last_contact=1700000300,
                longitude=13.0,
                latitude=52.5,
                geo_altitude=2000.0,
                on_ground=False,
                velocity=140.0,
                true_track=120.0,
                vertical_rate=-3.0,
                sensors=None,
                baro_altitude=1900.0,
                squawk="5678",
                spi=False,
                position_source=2,
                category=4,
            ),
            SimpleNamespace(
                icao24="ICAO3",
                callsign="CALL3",
                origin_country="FR",
                time_position=1700000400,
                last_contact=1700000500,
                longitude=2.0,
                latitude=48.0,
                geo_altitude=300.0,
                on_ground=True,
                velocity=50.0,
                true_track=30.0,
                vertical_rate=0.0,
                sensors=None,
                baro_altitude=250.0,
                squawk="4321",
                spi=True,
                position_source=0,
                category=2,
            ),
        ]
    )

def define_schema_silver() -> StructType:
    schema = StructType([
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", IntegerType(), True),
        StructField("last_contact", IntegerType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("geo_altitude", FloatType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", FloatType(), True),
        StructField("true_track", FloatType(), True),
        StructField("vertical_rate", FloatType(), True),
        StructField("sensors", StringType(), True),
        StructField("baro_altitude", FloatType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True),
        StructField("category", IntegerType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("altitude_diff", FloatType(), True),
        StructField("last_contact_h", StringType(), True),
        StructField("time_position_h", StringType(), True),
        StructField("vertical_category", StringType(), False),
        StructField("aircraft_category", StringType(), True),
        StructField("position_source_name", StringType(), True),
        StructField("isPoland", BooleanType(), True),
        
        

        
    ])
    return schema
def define_schema_bronze() -> StructType:
    schema = StructType([
        
        StructField("icao24", StringType(), True),
        StructField("callsign", StringType(), True),
        StructField("origin_country", StringType(), True),
        StructField("time_position", IntegerType(), True),
        StructField("last_contact", IntegerType(), True),
        StructField("longitude", FloatType(), True),
        StructField("latitude", FloatType(), True),
        StructField("geo_altitude", FloatType(), True),
        StructField("on_ground", BooleanType(), True),
        StructField("velocity", FloatType(), True),
        StructField("true_track", FloatType(), True),
        StructField("vertical_rate", FloatType(), True),
        StructField("sensors", StringType(), True),
        StructField("baro_altitude", FloatType(), True),
        StructField("squawk", StringType(), True),
        StructField("spi", BooleanType(), True),
        StructField("position_source", IntegerType(), True),
        StructField("category", IntegerType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),

    ])
    return schema
@pytest.fixture
def sample_df_bronze(get_spark):
    rows = [
        {
            "icao24": "ICAO1",
            "callsign": "CALL1",
            "origin_country": "PL",
            "time_position": 1700000000,
            "last_contact": 1700000100,
            "longitude": 19.0,
            "latitude": 52.0,
            "geo_altitude": 1000.0,
            "on_ground": False,
            "velocity": 120.0,
            "true_track": 90.0,
            "vertical_rate": 5.0,
            "sensors": None,
            "baro_altitude": 950.0,
            "squawk": "1234",
            "spi": False,
            "position_source": 1,
            "category": 3,
            "ingestion_timestamp": datetime.strptime("2026-09-19 15:29:47", "%Y-%m-%d %H:%M:%S"),
        },
        {
            "icao24": "ICAO2",
            "callsign": "CALL2",
            "origin_country": "DE",
            "time_position": 1700000200,
            "last_contact": 1700000300,
            "longitude": 13.0,
            "latitude": 52.5,
            "geo_altitude": 2000.0,
            "on_ground": False,
            "velocity": 140.0,
            "true_track": 120.0,
            "vertical_rate": -3.0,
            "sensors": None,
            "baro_altitude": 1900.0,
            "squawk": "5678",
            "spi": False,
            "position_source": 2,
            "category": 4,
            "ingestion_timestamp": datetime.strptime("2026-09-19 15:29:47", "%Y-%m-%d %H:%M:%S"),
        },
        {
            "icao24": "ICAO3",
            "callsign": "CALL3",
            "origin_country": "FR",
            "time_position": 1700000400,
            "last_contact": 1700000500,
            "longitude": 2.0,
            "latitude": 48.0,
            "geo_altitude": 300.0,
            "on_ground": True,
            "velocity": 50.0,
            "true_track": 30.0,
            "vertical_rate": 0.0,
            "sensors": None,
            "baro_altitude": 250.0,
            "squawk": "4321",
            "spi": True,
            "position_source": 0,
            "category": 2,
            "ingestion_timestamp": datetime.strptime("2026-09-19 15:29:47", "%Y-%m-%d %H:%M:%S"),
        },
        
    ]
    return get_spark.createDataFrame(rows, schema=define_schema_bronze())


@pytest.fixture
def sample_df(get_spark):
    rows = [
    {
        "icao24": "ICAO1",
        "callsign": "CALL1",
        "origin_country": "PL",
        "time_position": 1700000000,
        "last_contact": 1700000100,
        "longitude": 19.0,
        "latitude": 52.0,
        "geo_altitude": 1000.0,
        "on_ground": False,
        "velocity": 120.0,
        "true_track": 90.0,
        "vertical_rate": 5.0,
        "sensors": None,
        "baro_altitude": 950.0,
        "squawk": "1234",
        "spi": False,
        "position_source": 1,
        "category": 3,
        "ingestion_timestamp": datetime.strptime("2026-09-19 15:29:47", "%Y-%m-%d %H:%M:%S"),
        "altitude_diff": 50.0,
        "last_contact_h": "2026-09-19 15:15:00",
        "time_position_h": "2026-09-19 15:13:20",
        "vertical_category": "Climbing",
        "aircraft_category": "Medium",
        "position_source_name": "ADS-B",
        "isPoland": True,
    },
    {
        "icao24": "ICAO2",
        "callsign": "CALL2",
        "origin_country": "DE",
        "time_position": 1700000200,
        "last_contact": 1700000300,
        "longitude": 13.0,
        "latitude": 52.5,
        "geo_altitude": 2000.0,
        "on_ground": False,
        "velocity": 140.0,
        "true_track": 120.0,
        "vertical_rate": -3.0,
        "sensors": None,
        "baro_altitude": 1900.0,
        "squawk": "5678",
        "spi": False,
        "position_source": 2,
        "category": 4,
        "ingestion_timestamp": datetime.strptime("2026-09-19 15:29:47", "%Y-%m-%d %H:%M:%S"),
        "altitude_diff": 100.0,
        "last_contact_h": "2026-09-19 15:15:00",
        "time_position_h": "2026-09-19 15:16:40",
        "vertical_category": "Descending",
        "aircraft_category": "Large",
        "position_source_name": "ADS-B",
        "isPoland": True,
    },
    {
        "icao24": "ICAO3",
        "callsign": "CALL3",
        "origin_country": "FR",
        "time_position": 1700000400,
        "last_contact": 1700000500,
        "longitude": 2.0,
        "latitude": 48.0,
        "geo_altitude": 300.0,
        "on_ground": True,
        "velocity": 50.0,
        "true_track": 30.0,
        "vertical_rate": 0.0,
        "sensors": None,
        "baro_altitude": 250.0,
        "squawk": "4321",
        "spi": True,
        "position_source": 0,
        "category": 2,
        "ingestion_timestamp": datetime.strptime("2026-09-19 15:29:47", "%Y-%m-%d %H:%M:%S"),
        "altitude_diff": -50.0,
        "last_contact_h": "2026-09-19 15:15:00",
        "time_position_h": "2026-09-19 15:20:00",
        "vertical_category": "Constant Altitude",
        "aircraft_category": "Small",
        "position_source_name": "ADS-B",
        "isPoland": False,
    },
    ]
    return get_spark.createDataFrame(rows,schema=define_schema_silver())

@pytest.fixture
def sample_df_hist(get_spark):
    rows = [
        {
            "icao24": "ICAO1",
            "callsign": "CALL1",
            "origin_country": "PL",
            "time_position": 1700000000,
            "last_contact": 1700000100,
            "longitude": 19.0,
            "latitude": 52.0,
            "geo_altitude": 1000.0,
            "on_ground": False,
            "velocity": 120.0,
            "true_track": 90.0,
            "vertical_rate": 5.0,
            "sensors": None,
            "baro_altitude": 950.0,
            "squawk": "1234",
            "spi": False,
            "position_source": 1,
            "category": 3,
            "ingestion_timestamp": datetime.strptime("2026-09-18 15:15:47", "%Y-%m-%d %H:%M:%S"),
            "isPoland": True,
            "position_source_name": "ADS-B",
            "vertical_category": "Climbing",
            "altitude_diff": -50.0,
            "last_contact_h": "2026-09-19 15:15:00",
            "time_position_h": "2026-09-19 15:20:00",
            "aircraft_category": "Small",
        },
        {
            "icao24": "ICAO2",
            "callsign": "CALL2",
            "origin_country": "DE",
            "time_position": 1700000000,
            "last_contact": 1700000100,
            "longitude": 13.0,
            "latitude": 52.5,
            "geo_altitude": 2000.0,
            "on_ground": True,
            "velocity": 40.0,
            "true_track": 120.0,
            "vertical_rate": -3.0,
            "sensors": None,
            "baro_altitude": 1900.0,
            "squawk": "5678",
            "spi": False,
            "position_source": 2,
            "category": 4,
            "ingestion_timestamp": datetime.strptime("2026-09-18 15:30:47", "%Y-%m-%d %H:%M:%S"),
            "isPoland": True,
            "position_source_name": "ADS-B",
            "vertical_category": "Descending",
            "altitude_diff": -50.0,
            "last_contact_h": "2026-09-19 15:15:00",
            "time_position_h": "2026-09-19 15:20:00",
            "aircraft_category": "Small",
        },
    ]
    return get_spark.createDataFrame(rows,schema=define_schema_silver())