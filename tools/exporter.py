import random
import time
import logging
from logging.handlers import RotatingFileHandler
import threading
import queue

import os
from .mysql import get_conn

if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
handler = RotatingFileHandler('logs/export.log', maxBytes=100000, backupCount=3)
logger.addHandler(handler)

# 创建一个handler，用于将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 定义日志格式
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)


class WorkerThread(threading.Thread):
    def __init__(self, task_queue):
        threading.Thread.__init__(self)
        self.task_queue = task_queue

    def run(self):
        conn = get_conn()
        TABLE_NAME=os.getenv("TABLE_NAME")
        DEST=os.getenv("DEST")
        PT_KEY=os.getenv("PT_KEY")
        while True:
            try:
                task = self.task_queue.get(timeout=3)
                name = task['name']
                begin = task['begin']
                end = task['end']
                with_condition =  task['with_condition']
                sleep_time = random.uniform(0.01, 1.0)
                time.sleep(sleep_time)
                export_partition(conn, TABLE_NAME, DEST, PT_KEY,
                                 name, begin, end)
                self.task_queue.task_done()
            except queue.Empty:
                # 如果队列为空，跳出循环
                conn.close()
                break


def pick_key(partition_range_str):

    # 首先，根据"keys: ["分割字符串，然后取第二部分
    parts = partition_range_str.split("keys: [")
    key1_str = parts[1].split("];")[0]
    key2_str = parts[2].split("];")[0]
    return key1_str, key2_str


def export_partition(conn, table_name, dest: str, pt_key: str, pt_name: str, begin: str, end: str, with_condition=False):
    dest.endswith("/")
    path = dest + pt_name if dest.endswith("/") else f"{dest}/{pt_name}"
    AK=os.getenv("AK")
    SK=os.getenv("SK")
    AWS_REGION=os.getenv("AWS_REGION")
   
    if AK!="" and SK!="":
        command = f"""
            INSERT INTO 
            FILES(
                "path" = "{path}",
                "format" = "csv",
                "csv.column_separator"="|",
                "compression" = "uncompressed",
                "aws.s3.access_key" = {AK},
                "aws.s3.secret_key" = {SK},
                "target_max_file_size" = "104857600",
                "aws.s3.region" = "{AWS_REGION}"
            )
            """
    else:
        command = f"""
            INSERT INTO 
            FILES(
                "path" = "{path}",
                "format" = "csv",
                "csv.column_separator"="|",
                "compression" = "uncompressed",
                "target_max_file_size" = "104857600",
                "aws.s3.region" = "{AWS_REGION}"
            )
            """
    if with_condition:
        INSERT_KEY=os.getenv("INSERT_KEY")
        INSERT_VALUE=os.getenv("INSERT_VALUE")
        choice = f"""
        SELECT * FROM {table_name}
        where {pt_key} >= "{begin}" and {pt_key} < "{end}"
        and {INSERT_KEY} >= "{INSERT_VALUE}";
        """
    else:
        choice = f"""
        SELECT * FROM {table_name}
        where {pt_key} >= "{begin}" and {pt_key} < "{end}";
        """

    command = command +  choice

    logger.info(f"begin export {begin}==>{end}")
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()
            logger.info(f"success to export {begin}==>{end}")
    except Exception as ex:
        logger.error(f"faild to export {begin}==>{end}")
        logger.error(ex)


def get_tasks():
    TABLE_NAME=os.getenv("TABLE_NAME")
    PT_KEY=os.getenv("PT_KEY")
    conn = get_conn()
    
    cmd_partition = f"SHOW PARTITIONS FROM {TABLE_NAME}"
    partitions = list()
    with conn.cursor() as cursor:
        sql = str(cmd_partition)
        cursor.execute(sql)
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            if row['PartitionKey'] != PT_KEY:
                logger.warning(
                    f'Partition {row["PartitionName"]}:{row["PartitionKey"]} not match partition key {PT_KEY}')
                continue

            begin, end = pick_key(row["Range"])
            partitions.append(
                {
                    "name": row["PartitionName"],
                    "begin": begin,
                    "end": end
                }
            )
    conn.close()
    return partitions

def run(with_condition=False):
    CONCURRENCY=os.getenv("CONCURRENCY")

    task_queue = queue.Queue()  # 创建任务队列
    # 向队列中添加任务
    partitions = get_tasks()
    for task in partitions:
        task['with_condition'] = with_condition
        task_queue.put(task)
    # 创建并启动线程

    threads = []
    for i in range(0, int(CONCURRENCY)):
        thread = WorkerThread(task_queue)
        threads.append(thread)
        thread.start()

    # 等待队列中的所有任务完成
    task_queue.join()

    # 所有任务完成后，停止线程
    for thread in threads:
        thread.join()

    logger.info(f"ALL TASK DONE !!! bingo!")