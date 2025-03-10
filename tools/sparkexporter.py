from pyspark.sql import SparkSession
import os
from random import randint
import time

MIN_COUNT = 1000


def find_data_filter_by_table(name: str) -> str:
    table_names = os.getenv("TABLE_NAME").split(",")
    data_filters = os.getenv("DATA_FILTER").split(",")
    check = len(data_filters)
    for i in range(0, len(table_names)):
        if table_names[i] == name:
            if i < check:
                return data_filters[i]
    return ""


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
    # .config("spark.hadoop.fs.s3a.list.version", "2") \
    # .config("parquet.enable.summary-metadata", "false") \
    # .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    # .config("fs.s3a.fast.upload", "true") \


def get_spark(job_name: str, table_name: str, index):
    dependency_jars = os.getenv("DEPENDENCY_JARS")
    spark_cache = os.getenv("SPARK_CACHE")
    spark = SparkSession.builder.appName(f"StarRocksMigration{job_name}{table_name}{index}") \
        .config("spark.jars", dependency_jars) \
        .config("spark.scheduler.mode", "FIFO") \
        .config("spark.local.dir", spark_cache) \
        .config("spark.memory.offHeap.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryo.registrationRequired", "false") \
        .config("spark.memory.offHeap.size", "12g") \
        .config("spark.driver.memory", "8g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.dynamicAllocation.enabled", "true") \
        .getOrCreate()
    return spark


def run(spark, job_name: str, table_name: str, partition, logger):
    pt_name = partition["name"]

    if partition["ptype"] == "list":
        ptv = partition['start']
        ptvs = ptv.split(",")
        if len(ptvs) == 1:
            filter_str = f"{partition['key']} = {ptv}"
        else:
            filter_str = f"{partition['key']} in ({ptv})"

    else:
        ptv = partition['start']
        ptv2 = partition['end']
        if partition["type"] == "number":
            filter_str = f"{partition['key']}>={ptv} and {partition['key']} < {ptv2}"
        else:
            filter_str = f"{partition['key']}>='{ptv}' and {partition['key']} < '{ptv2}'"

    data_filter = find_data_filter_by_table(table_name)
    if data_filter:
        filter_str = f"{filter_str} and {data_filter}"

    logger.info(f"[exporter][{job_name}]===>BEGIN RUN {table_name}==>{pt_name} with {filter_str}!")
    try:
        output = runp(spark, job_name, table_name, filter_str, partition, logger)
        time.sleep(1)
        logger.info(f"[exporter][{job_name}]===>SUCCESS RUN {table_name}==>{pt_name}!")
        return output
    except Exception as ex:
        logger.error(f"[exporter][{job_name}]===>FAILED TO RUN {table_name}==>{pt_name} due to {ex}")
    finally:
        spark.catalog.clearCache()


def runp(spark: SparkSession, job_name: str, table_name: str, filter_str: str, partition, logger):
    # 创建 SparkSession
    # 使用 StarRocks 数据源读取数据
    host, port, user, pwd, db_name = get_data_source()
    storage = os.getenv("STORAGES")
    starrocks_table_size = int(os.getenv("STARROCKS_TABLE_SIZE", "10000"))
    max_row_count = int(os.getenv("PER_FILE_MAX_ROW_COUNT"))

    starrocks_df = spark.read.format("starrocks") \
        .option("starrocks.table.identifier", f"{db_name}.{table_name}") \
        .option("starrocks.fe.http.url", f"{host}:8030") \
        .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{host}:{port}") \
        .option("starrocks.user", f"{user}") \
        .option("starrocks.password", f"{pwd}") \
        .option("starrocks.exec.mem.limit", 4294967296) \
        .option("starrocks.batch.size", 10000)

    if filter_str:
        logger.warn(f"[exporter] filter data in {table_name} {filter_str}")
        starrocks_df = starrocks_df.option("starrocks.filter.query", filter_str)

    if partition :
        pt_name = partition["name"]
        starrocks_df = starrocks_df.load()
        row_count = partition["rowcount"]
        # 统计得来的数字，不准确
        if row_count < 1000:
            # 可能是空数据
            row_count = starrocks_df.count()

        if row_count == 0:
            logger.warn(f"[exporter]NOT DATA in {table_name} {pt_name}")
            # 空数据无需导出
            return ""
    else:
        starrocks_df = starrocks_df.load()

    direct_im = os.getenv("SPARK_DIRECT") == "True"

    if not direct_im:
        if storage.startswith("s3://"):
            storage = storage.replace("s3://", "s3a://")

            # s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
            # 例如 s3://tx-au-mock-data/sunexf/test1/sunim/data_point_val/p20231103/
            # data_01add602-b21d-11ef-b192-0ac76da15273_0_1_0_2_0.csv

        local_dir = os.getenv("SPARK_TEMP")

        if partition:
            pt_name = partition["name"]
            local_path = local_dir + f"/{job_name}/{db_name}/{table_name}/{pt_name}/"
            logger.info(f"[exporter]Begin to {table_name} {pt_name} with {local_path}")
        else:
            local_path = local_dir + f"/{job_name}/{db_name}/{table_name}/default/"
            logger.info(f"[exporter]Begin to {table_name} default with {local_path}")

        starrocks_df.coalesce(10).write \
            .option("header", "false") \
            .option("maxRecordsPerFile", max_row_count) \
            .format("parquet") \
            .mode("overwrite") \
            .save(local_path)

        time.sleep(2)
        return local_path
    else:
        im_host, im_port, im_user, im_pwd, im_db_name = get_data_source()
        starrocks_df.coalesce(10).write.format("starrocks") \
            .option("starrocks.table.identifier", f"{im_db_name}.{table_name}") \
            .option("starrocks.fe.http.url", f"{im_host}:8030") \
            .option("starrocks.fe.jdbc.url", f"jdbc:mysql://{im_host}:{im_port}") \
            .option("starrocks.user", f"{im_user}") \
            .option("starrocks.password", f"{im_pwd}") \
            .mode("append") \
            .save()


def runone(job_name: str, table_name: str, logger):
    spark = get_spark(job_name, table_name, index=0)
    filter_str = ""
    data_filter = find_data_filter_by_table(table_name)
    if data_filter:
        filter_str = data_filter
    return runp(spark, job_name, table_name, filter_str, dict(), logger)
