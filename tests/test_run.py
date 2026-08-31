"""
This module contains example tests for a Kedro project.
Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py.
"""
from pathlib import Path
import pyspark.sql.functions as F 
from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

# The tests below are here for the demonstration purpose
# and should be replaced with the ones testing the project
# functionality

# class TestKedroRun:
#     def test_kedro_run(self):
#         bootstrap_project(Path.cwd())

#         with KedroSession.create(project_path=Path.cwd()) as session:
#             assert session.run() is not None
# def test_silver_pipeline_smoke(spark):
#     df = spark.createDataFrame(
#         [
#             ("icao1", "CALL1", "PL", 19.0, 52.0, 1000.0, 500.0, True, 10.0, 1, 0, 1),
#             ("icao2", "CALL2", "DE", 19.1, 52.1, 1200.0, 600.0, False, 20.0, 2, 0, 2),
#         ],
#         ["icao24", "callsign", "origin_country", "longitude", "latitude", "geo_altitude", "baro_altitude", "on_ground", "velocity", "position_source", "category", "spi"]
#     )

#     out = (
#         df.withColumn("isPoland", F.lit(True))
#         .withColumn("vertical_category", F.lit("ok"))
#     )

#     assert out.columns
#     assert out.count() == 2
#     assert "isPoland" in out.columns
#     assert "vertical_category" in out.columns