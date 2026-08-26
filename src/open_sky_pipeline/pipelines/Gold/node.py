from open_sky_pipeline.Connect import get_spark
from pyspark.sql.functions import col,avg,count
import json
from pyspark.sql import DataFrame,SparkSession
from delta.tables import DeltaTable
from kedro.config import OmegaConfigLoader
from kedro.framework.project import settings
from pathlib import Path

def gold_layer(*args, **kwargs):
    spark = SparkSession.builder.getOrCreate()
    def _write_to_gold(gold_df:DataFrame,gold_path:str):
        if not DeltaTable.isDeltaTable(spark, gold_path):
            # pierwszy load
            gold_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(gold_path)
        else:
            # kolejne loady
            gold = DeltaTable.forPath(spark, gold_path)

            gold.alias("target") \
                .merge(
                    gold_df.alias("source"),
                    'target.ingestion_timestamp = source.ingestion_timestamp',
                    
                ) \
                .whenNotMatchedInsertAll() \
                .execute()
    def _write_to_db(df: DataFrame,target_table:str):
        project_root = Path(__file__).resolve().parents[4]
        conf_path = str(project_root / settings.CONF_SOURCE)
        conf_loader = OmegaConfigLoader(conf_source=conf_path)

        credentials = conf_loader["credentials"]
        credentials = credentials['postgres']
        jdbc_url = "jdbc:postgresql://localhost:5432/OpenSky"

        df.write \
        .mode("overwrite") \
        .jdbc(url=jdbc_url, table=target_table, properties=credentials)
    # 
    df=spark.read.format('delta').load("s3a://meddalion/silver/aircraft").where((col('isPoland')==True) & (col('position_source_name')=='ADS-B'))
    df_hist=spark.read.format('delta').load("s3a://meddalion/silver/aircraft_hist").where((col('isPoland')==True) & (col('position_source_name')=='ADS-B'))

    df_fact=df.select('callsign','last_contact_h','time_position_h','longitude','latitude','geo_altitude','baro_altitude','altitude_diff','true_track','velocity','vertical_category','on_ground','squawk', 'spi')
    df_aircraft_dim=df.select('icao24','origin_country','aircraft_category')

    # historical data
    df_all=df_hist.groupBy(col('ingestion_timestamp')).count().orderBy(col('count'),ascending=False).select('ingestion_timestamp',col('count').alias('all_observation_count'))
    df_ground=df_hist.where(col('on_ground')==True ).groupBy(col('ingestion_timestamp')).count().orderBy([col('ingestion_timestamp')],ascending=False).select('ingestion_timestamp',col('count').alias('on_ground_count'))
    df_velocity=df_hist.where(col('on_ground')==False).groupBy(col('ingestion_timestamp')).agg(
        avg('velocity').alias('avg_velocity'),
        avg('baro_altitude').alias('avg_baro_alt'),
        count('*').alias('count_flying')
        ).orderBy(col('ingestion_timestamp'),ascending=False).select('ingestion_timestamp','avg_velocity','avg_baro_alt','count_flying')
    ##################
    df_all=df_all.alias('da')\
    .join(df_ground.alias('dg'),on=["ingestion_timestamp"],how="left")\
    .join(df_velocity.alias('dv'),on=["ingestion_timestamp"],how="left")\
    .select('ingestion_timestamp','all_observation_count','count_flying','on_ground_count','dv.avg_velocity','dv.avg_baro_alt')
    ##################
    df_cat=df_hist.where(col('on_ground')==False)\
    .groupBy(col('ingestion_timestamp'),col('vertical_category'))\
    .agg(
        avg('baro_altitude').alias('avg_baro_alt'),
        avg('velocity').alias('avg_velocity'),
        count('*').alias('flying_count')
        )\
    .orderBy([col('ingestion_timestamp'),col('vertical_category')],ascending=False)


        
    #################

    df_fact.write.mode('overwrite').format('delta').save("s3a://meddalion/gold/fact_table")
    target_table = DeltaTable.forPath(spark, "s3a://meddalion/gold/dim_table")
    target_table.alias("target") \
    .merge(
        df_aircraft_dim.alias("source"),
        "target.icao24 = source.icao24" #and 'target.ingestion_timestamp = source.ingestion_timestamp',
        
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    # _write_to_gold(df_aircraft_dim,"s3a://meddalion/gold/dim_table")
    _write_to_gold(df_all,"s3a://meddalion/gold/KPI_for_timestamps")
    _write_to_gold(df_cat,"s3a://meddalion/gold/KPI_for_cat&ts")
    
    _write_to_db(df_fact,'fact_table')
    _write_to_db(df_aircraft_dim,'dim_table')
    _write_to_db(df_all,'kpi_ts')
    _write_to_db(df_cat,'kpi_cat_ts')
    return True