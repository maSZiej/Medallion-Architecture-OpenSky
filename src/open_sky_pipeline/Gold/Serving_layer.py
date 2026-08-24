from open_sky_pipeline.Connect import get_spark
import json
from pyspark.sql import DataFrame

def write_to_db(df: DataFrame,target_table:str):
    jdbc_url = "jdbc:postgresql://localhost:5432/OpenSky"
    with open("db.json", "r") as f:
        connection_properties = json.load(f)

    df.write \
    .mode("overwrite") \
    .jdbc(url=jdbc_url, table=target_table, properties=connection_properties)
    

spark=get_spark()

df_fact=spark.read.format('delta').load("s3a://meddalion/gold/fact_table")
df_dim=spark.read.format('delta').load("s3a://meddalion/gold/dim_table")
df_kpi_ts=spark.read.format('delta').load("s3a://meddalion/gold/KPI_for_timestamps")
df_kpi_cat_ts=spark.read.format('delta').load("s3a://meddalion/gold/KPI_for_cat&ts")

write_to_db(df_fact,'fact_table')
write_to_db(df_dim,'dim_table')
write_to_db(df_kpi_ts,'kpi_ts')
write_to_db(df_kpi_cat_ts,'kpi_cat_ts')