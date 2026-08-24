from open_sky_pipeline.Connect import get_spark
from pyspark.sql.functions import col,avg,count

spark=get_spark()
df=spark.read.format('delta').load("s3a://silver/aircraft").where((col('isPoland')==True) & (col('position_source_name')=='ADS-B'))
df_hist=spark.read.format('delta').load("s3a://silver/aircraft_hist").where((col('isPoland')==True) & (col('position_source_name')=='ADS-B'))

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
.select('ingestion_timestamp','all_observation_count','count_flying','on_ground_count','dv.avg_velocity','dv.avg_baro_alt').show()
##################
df_cat=df_hist.where(col('on_ground')==False)\
.groupBy(col('ingestion_timestamp'),col('vertical_category'))\
.agg(
    avg('baro_altitude').alias('avg_baro_alt'),
    avg('velocity').alias('avg_velocity'),
    count('*').alias('flying_count')
    )\
.orderBy([col('ingestion_timestamp'),col('vertical_category')],ascending=False)