from pyspark.sql import SparkSession
import os
from random import randint
import time

MIN_COUNT = 1000


def get_data_source(cluster_type="target"):
    host_str = os.getenv("SOURCE_HOST") if cluster_type == "source" else os.getenv("TARGET_HOST")
    hosts = host_str.split(",")
    index = randint(0, len(hosts) - 1)
    host_ip = hosts[index]
    if cluster_type == "source":
        return host_ip, os.getenv("SOURCE_PORT"), os.getenv("SOURCE_USER"), os.getenv("SOURCE_PWD"), os.getenv(
            "SOURCE_DB_NAME")
    else:
        return host_ip, os.getenv("TARGET_PORT"), os.getenv("TARGET_USER"), os.getenv("TARGET_PWD"), os.getenv(
            "TARGET_DB_NAME")


def get_spark(job_name: str, index: int):
    spark_cache = os.getenv("SPARK_CACHE")
    dependency_jars = os.getenv("DEPENDENCY_JARS")
    CONCURRENCY = int(os.getenv("EXPORT_CONCURRENCY"))

    spark = SparkSession.builder.appName(f"StarRocksMigration{job_name}{index}") \
        .config("spark.jars", dependency_jars) \
        .config("spark.scheduler.mode", "FIFO") \
        .config("spark.local.dir", spark_cache) \
        .config("spark.scheduler.allocation.maxConcurrent", f"{CONCURRENCY}") \
        .config("spark.sql.files.maxPartitionBytes", "128m") \
        .getOrCreate()
    return spark


def run(spark, job_name: str, table_name: str, file_path: str, columns: list, logger):
    host, port, user, pwd, db_name = get_data_source()
    if file_path.startswith("s3://"):
        file_path = file_path.replace("s3://", "s3a://")
    logger.info(f"begin to process {file_path}")
    df = spark.read.parquet(file_path)
    row_count = df.count()

    if row_count == 0:
        logger.info(f"[importer]ingore to write {file_path} due to {row_count} rows")
        return True, ""
    else:
        logger.info(f"[importer]begin to {file_path}  with {row_count} rows")

    try:
        df.write.format("starrocks") \
            .option("starrocks.table.identifier", f"{db_name}.{table_name}") \
            .option("starrocks.fe.http.url", f"{host}:8030") \
            .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{host}:{port}") \
            .option("starrocks.user", f"{user}") \
            .option("starrocks.password", f"{pwd}") \
            .option("starrocks.columns", ",".join(columns)) \
            .mode("append") \
            .save()
        logger.info(f"[importer][{job_name}]===>Succeed in importing {table_name} from {file_path}")
        return True, ""
    except Exception as ex:
        logger.error(f"[importer][{job_name}]===>Failed to import {ex}")
        return False, str(ex)
