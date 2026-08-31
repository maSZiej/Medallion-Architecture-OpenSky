import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from kedro.framework.hooks import hook_impl
from pathlib import Path
class SparkHooks:
    @hook_impl
    def after_context_created(self, context) -> None:
        """Initialises a SparkSession using the config defined in project's conf folder."""

        PACKAGES = [
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "org.postgresql:postgresql:42.7.3",
        ]
        
        # Warto upewnić się, że ścieżka do winutils.exe istnieje pod C:\hadoop\bin\winutils.exe
        # HADOOP_HOME = r"C:\hadoop"
        
        project_root = Path(__file__).resolve().parents[2]
        HADOOP_HOME = f"{project_root}/jars/hadoop"
        os.environ["HADOOP_HOME"] = HADOOP_HOME
        os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")
        # os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-17.0.2"
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        builder = (
            SparkSession.builder
            .appName(context.project_path.name)
            .master("local[*]")
            # ---- S3A / MinIO Connection ----
            .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT", "http://minio:9000"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            # ---- Timeouts & Stability ----
            .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
            .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
            .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
            # ---- Delta Lake ----
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            # ---- Performance ----
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")\
            ### ---- Driver ----
            .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/tmp/spark") \
        )

        spark = configure_spark_with_delta_pip(
            builder,
            extra_packages=PACKAGES
        ).getOrCreate()


        h_conf = spark.sparkContext._jsc.hadoopConfiguration()
        h_conf.set("fs.s3a.threads.keepalivetime", "60000")
        h_conf.set("fs.s3a.multipart.purge.age", "86400000")
