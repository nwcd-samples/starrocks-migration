from pyspark.sql import SparkSession
import os
from random import randint
import time

def get_data_source(cluster_type="source"):
    host_str =os.getenv("SOURCE_HOST") if cluster_type == "source" else os.getenv("TARGET_HOST")
    hosts = host_str.split(",")
    index = randint(0,len(hosts)-1)
    host_ip = hosts[index]
    if cluster_type == "source":
        return host_ip,os.getenv("SOURCE_PORT"),os.getenv("SOURCE_USER"),os.getenv("SOURCE_PWD"),os.getenv("SOURCE_DB_NAME")
    else:
        return host_ip,os.getenv("TARGET_PORT"),os.getenv("TARGET_USER"),os.getenv("TARGET_PWD"),os.getenv("TARGET_DB_NAME")

def run(job_name:str,table_name:str, partitions:list, logger):
    CONCURRENCY = int(os.getenv("EXPORT_CONCURRENCY"))
    dependency_jars=os.getenv("DEPENDENCY_JARS")

    for partition in partitions:
        pt_name = partition["name"]
        spark = SparkSession.builder.appName(f"StarRocksMigration{job_name}{table_name}{pt_name}") \
        .config("spark.jars", dependency_jars) \
        .config("spark.scheduler.mode", "FIFO") \
        .config("spark.scheduler.allocation.maxConcurrent", f"{CONCURRENCY}") \
        .getOrCreate()
        
        if partition["ptype"] == "list":
            ptv = partition['start']
            filter_str = f"{partition['key']}={ptv}"
        else:
            ptv = partition['start']
            ptv2 = partition['end']
            if partition["type"] == "number":
                filter_str = f"{partition['key']}>={ptv} and {partition['key']} < {ptv2}"
            else:
                filter_str = f"{partition['key']}>='{ptv}' and {partition['key']} < '{ptv2}'"
        logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}==>{pt_name}!")
        try:
            runp(spark, job_name, table_name,filter_str,pt_name,logger)
            time.sleep(2)
            logger.info(f"[exporter][{job_name}]===>SUCCESS RUN {table_name}==>{pt_name}!")
        except Exception as ex:
            logger.error(f"[exporter][{job_name}]===>FAILED TO RUN {table_name}==>{pt_name} due to {ex}")
        finally:
            spark.catalog.clearCache()
    
        spark.stop()

def runp(spark:SparkSession,job_name:str, table_name:str,filter_str:str, pt_name:str, logger):
    # 创建 SparkSession
    # 使用 StarRocks 数据源读取数据
    host, port,user,pwd,db_name = get_data_source()
    storage = os.getenv("STORAGES")
    max_row_count = int(os.getenv("PER_FILE_MAX_ROW_COUNT"))
    logger.info(f"begin partition {pt_name} with {filter_str}")
    
    starrocksSparkDF = spark.read.format("starrocks") \
        .option("starrocks.table.identifier", f"{db_name}.{table_name}") \
        .option("starrocks.fe.http.url", f"{host}:8030") \
        .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{host}:{port}") \
        .option("starrocks.user", f"{user}") \
        .option("starrocks.password", f"{pwd}") \
        .option("starrocks.filter.query", filter_str) \
        .load()

    if storage.startswith("s3://"):
        storage = storage.replace("s3://", "s3a://")

    # s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
    # 例如 s3://tx-au-mock-data/sunexf/test1/sunim/data_point_val/p20231103/data_01add602-b21d-11ef-b192-0ac76da15273_0_1_0_2_0.csv
    s3_path = storage + f"{job_name}/{db_name}/{table_name}/{pt_name}/" if storage.endswith("/") else f"{storage}/{job_name}/{db_name}/{table_name}/{pt_name}/"
    starrocksSparkDF.write \
            .option("header", "false") \
            .option("delimiter", "|#") \
            .option("maxRecordsPerFile", max_row_count) \
            .format("csv") \
            .mode("overwrite") \
            .save(s3_path)
    # 强制执行
    starrocksSparkDF.show(1)

    