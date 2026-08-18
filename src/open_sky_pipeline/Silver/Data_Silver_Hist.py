from open_sky_pipeline.Connect import get_spark

spark=get_spark()
df3_hist = spark.read.parquet("s3a://silver/aircraft")
try:
    df3_hist.write \
        .format("delta") \
        .mode("append") \
        .save("s3a://silver/aircraft_hist")
except Exception as e:
    print(e)