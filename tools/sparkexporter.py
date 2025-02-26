from pyspark.sql import SparkSession
import os
from random import randint
import time

MIN_COUNT = 1000


def get_data_source(cluster_type="source"):
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

    # .config("spark.executor.cores", 4) \
    # .config("spark.executor.instances", 4) \
    # .config("spark.executor.memory", "2g") \
    # .config("spark.driver.memory", "8g") \
    # .config("spark.default.parallelism", "32") \
    # .config("spark.sql.shuffle.partitions", "32") \
    # .config("spark.sql.files.maxPartitionBytes", "134217728") \
    # .config("spark.hadoop.fs.s3a.block.size", "536870912") \
    # .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    # .config("spark.dynamicAllocation.enabled", "false") \
    # .config("spark.executor.memoryOverhead", "1024") \
    # .config("spark.hadoop.fs.s3a.connection.maximum", "100") \
    # .config("spark.hadoop.fs.s3a.connection.timeout", "30000") \
    #.config("spark.hadoop.fs.s3a.list.version", "2") \
    #.config("parquet.enable.summary-metadata", "false") \
    #.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    #.config("fs.s3a.fast.upload", "true") \
def get_spark(job_name: str, table_name: str, index):
    dependency_jars = os.getenv("DEPENDENCY_JARS")
    spark_cache = os.getenv("SPARK_CACHE")
    spark = SparkSession.builder.appName(f"StarRocksMigration{job_name}{table_name}{index}") \
    .config("spark.jars", dependency_jars) \
    .config("spark.scheduler.mode", "FIFO") \
    .config("spark.local.dir", spark_cache) \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "2g") \
    .getOrCreate()
    return spark


def run(spark, job_name: str, table_name: str, partition, logger):
    if partition["rowcount"] == 0:
        logger.warn(f"[exporter][{job_name}]===>NO DATA IN {table_name}==>{pt_name}!")
        return

    pt_name = partition["name"]

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
        output = runp(spark, job_name, table_name, filter_str, pt_name, logger)
        time.sleep(2)
        logger.info(f"[exporter][{job_name}]===>SUCCESS RUN {table_name}==>{pt_name}!")
        return output
    except Exception as ex:
        logger.error(f"[exporter][{job_name}]===>FAILED TO RUN {table_name}==>{pt_name} due to {ex}")
    finally:
        spark.catalog.clearCache()




def runp(spark: SparkSession, job_name: str, table_name: str, filter_str: str, pt_name: str, logger):
    # 创建 SparkSession
    # 使用 StarRocks 数据源读取数据
    host, port, user, pwd, db_name = get_data_source()
    storage = os.getenv("STORAGES")
    max_row_count = int(os.getenv("PER_FILE_MAX_ROW_COUNT"))

    starrocksSparkDF = spark.read.format("starrocks") \
        .option("starrocks.table.identifier", f"{db_name}.{table_name}") \
        .option("starrocks.fe.http.url", f"{host}:8030") \
        .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{host}:{port}") \
        .option("starrocks.user", f"{user}") \
        .option("starrocks.password", f"{pwd}")

    if pt_name and filter_str:
        logger.info(f"begin partition {pt_name} with {filter_str}")
        starrocksSparkDF = starrocksSparkDF.option("starrocks.filter.query", filter_str)

    starrocksSparkDF = starrocksSparkDF.load()

    # 强制执行
    directim= os.getenv("SPARK_DIRECT")=="True"
    if not directim:
        if storage.startswith("s3://"):
            storage = storage.replace("s3://", "s3a://")

            # s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
            # 例如 s3://tx-au-mock-data/sunexf/test1/sunim/data_point_val/p20231103/data_01add602-b21d-11ef-b192-0ac76da15273_0_1_0_2_0.csv
        
        
        localdir= os.getenv("SPARK_TEMP")


        if pt_name:
            testlocal = localdir + f"/{job_name}/{db_name}/{table_name}/{pt_name}/"
            logger.info(f"[exporter]begin to {table_name} {pt_name} with {testlocal}")
        else:
            testlocal = localdir + f"/{job_name}/{db_name}/{table_name}/default/"
            logger.info(f"[exporter]begin to {table_name} default with {testlocal}")
        starrocksSparkDF.coalesce(100).write \
            .option("header", "false") \
            .option("maxRecordsPerFile", max_row_count) \
            .format("parquet") \
            .mode("overwrite") \
            .save(testlocal)

        time.sleep(5)
        return testlocal
    else:
        im_host, im_port,im_user,im_pwd,im_db_name = get_data_source()
        starrocksSparkDF.write.format("starrocks") \
            .option("starrocks.table.identifier", f"{im_db_name}.{table_name}") \
            .option("starrocks.fe.http.url", f"{im_host}:8030") \
            .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{im_host}:{im_port}") \
            .option("starrocks.user", f"{im_user}") \
            .option("starrocks.password", f"{im_pwd}") \
            .mode("append") \
            .save()

        # 写入后，s3形成文件是异步行为，需要时间
        # 简单根据行数做一定待定，保证完毕时间
        # if int(row_count/1000000) > 1:
        #     time.sleep(int(row_count/1000000))

def runone(job_name: str, table_name: str, logger):
    CONCURRENCY = int(os.getenv("EXPORT_CONCURRENCY"))
    dependency_jars = os.getenv("DEPENDENCY_JARS")
    spark_cache = os.getenv("SPARK_CACHE")
    spark = SparkSession.builder.appName(f"StarRocksMigration{job_name}{table_name}") \
        .config("spark.jars", dependency_jars) \
        .config("spark.scheduler.mode", "FIFO") \
        .config("spark.local.dir", spark_cache) \
        .config("spark.scheduler.allocation.maxConcurrent", f"{CONCURRENCY}") \
        .getOrCreate()

    runp(spark, job_name, table_name, "", "", logger)
