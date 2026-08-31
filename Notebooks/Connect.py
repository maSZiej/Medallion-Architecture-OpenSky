import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import sys
from pathlib import Path


PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.7.3",
]
# minio_access_key = 'minioadmin'
# minio_secret_key = 'minioadmin'
# minio_endpoint = 'http://127.0.0.1:9000'
project_root = Path(__file__).resolve().parents[1]
# print(project_root)
HADOOP_HOME = f"{project_root}/jars/hadoop"

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = (
    os.path.join(HADOOP_HOME, "bin")
    + os.pathsep
    + os.environ["PATH"]
)
os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.2"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# print("Python:", sys.executable)
# print("Version:", sys.version)
# print("VIRTUAL_ENV:", os.environ.get("VIRTUAL_ENV"))
# print("SPARK_HOME:", os.environ.get("SPARK_HOME"))
# print("JAVA_HOME:", os.environ.get("JAVA_HOME"))

######################################
def get_spark(app_name: str = "incremental_data"):
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # ---- S3A / MinIO ----
        .config(
            "spark.hadoop.fs.s3a.endpoint",os.getenv("S3_ENDPOINT", "http://localhost:9000")
        )
        .config("spark.hadoop.fs.s3a.access.key",os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key",os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        # ---- Delta Lake ----
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.databricks.delta.schema.autoMerge.enabled","true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    spark = configure_spark_with_delta_pip(
        builder,
        extra_packages=PACKAGES
    ).getOrCreate()

    # ---- Safety net: JVM-level override ----
    h_conf = spark.sparkContext._jsc.hadoopConfiguration()
    h_conf.set("fs.s3a.connection.timeout", "60000")
    h_conf.set("fs.s3a.socket.timeout", "60000")
    h_conf.set("fs.s3a.threads.keepalivetime", "60000")
    h_conf.set("fs.s3a.connection.establish.timeout", "60000")
    h_conf.set(
    "fs.s3a.aws.credentials.provider",
    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    h_conf.set("fs.s3a.multipart.purge.age", "86400000")
    # spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.multipart.purge.age", "86400000")

    return spark





    # for entry in h_conf.iterator():
    #     key = entry.getKey()
    #     value = entry.getValue()

    #     if "timeout" in key.lower() or "s3a" in key.lower():
    #         print(f"{key} = {value}")
    # spark.sparkContext.setLogLevel("WARN")
    # print("Spark started successfully")
