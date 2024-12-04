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
        DB_NAME=os.getenv("DB_NAME")
        TABLE_NAME=os.getenv("TABLE_NAME")
        DEST=os.getenv("DEST")
        PT_KEY=os.getenv("PT_KEY")
        AK=os.getenv("AK")
        SK=os.getenv("SK")
        AWS_REGION=os.getenv("AWS_REGION")
        if AK =="" or SK== "":
            DEST = DEST.replace("s3:", "s3a:")

        while True:
            try:
                task = self.task_queue.get(timeout=3)
                pt_name = task['name']
                # begin = task['begin']
                # end = task['end']
                # with_condition =  task['with_condition']
                sleep_time = random.uniform(0.01, 1.0)
                time.sleep(sleep_time)
                export_partition(conn, DB_NAME, TABLE_NAME, DEST, PT_KEY,
                                 pt_name, AWS_REGION,AK, SK)
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


def check_count(conn):
    JOB_CMD1="SHOW EXPORT WHERE STATE = 'PENDING'"
    JOB_CMD2="SHOW EXPORT WHERE STATE = 'EXPORTING'"
    counter1 = 0
    counter2 = 0
    with conn.cursor() as cursor:
        cursor.execute(JOB_CMD1)
        conn.commit()
        rows = cursor.fetchall()
        counter1 = len([i for i in rows])

    with conn.cursor() as cursor:
        cursor.execute(JOB_CMD2)
        conn.commit()
        rows = cursor.fetchall()
        counter2 = len([i for i in rows])
        

     # 先等10秒再说
            status = ""
            if status == 'FINISHED':
                logger.info(f"success to import {file_path} to {table_name}")
                break
            elif status == 'CANCELLED':
                logger.info(f"failed to import {file_path} to {table_name}")
                break
            else:
                time.sleep(10)

        
def export_partition(conn, db_name, table_name, dest: str, pt_key: str, pt_name: str,aws_region:str, ak:str, sk:str):
    dest.endswith("/")
    path = dest + pt_name if dest.endswith("/") else f"{dest}/{pt_name}"
    
   
    if ak!="" and sk!="":
        command = f"""
            EXPORT TABLE {db_name}.{table_name} 
            PARTITION ({pt_name})
            TO "{path}" 
            PROPERTIES
            (
                "column_separator"="|",
                "timeout" = "3600"
            )
            WITH BROKER
            (
                "aws.s3.access_key" = "{ak}",
                "aws.s3.secret_key" = "{sk}",
                "aws.s3.region" = "{aws_region}"
            )
            """
    else:
        command = f"""
            EXPORT TABLE {db_name}.{table_name} 
            PARTITION ({pt_name})
            TO "{path}" 
            PROPERTIES
            (
                "column_separator"="|",
                "timeout" = "3600"
            )
            WITH BROKER
            (
                "aws.s3.region" = "{aws_region}"
            )
            """
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()
        
        JOB_CMD = "SELECT LAST_QUERY_ID() as result"
        with conn.cursor() as cursor:
            cursor.execute(JOB_CMD)
            conn.commit()
            t = cursor.fetchone()
            return t['result']

    except Exception as ex:
        logger.error(f"faild to export {pt_name}")
        logger.error(ex)
        return ""


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
    conn = get_conn()
    DB_NAME=os.getenv("DB_NAME")
    TABLE_NAME=os.getenv("TABLE_NAME")
    DEST=os.getenv("DEST")
    PT_KEY=os.getenv("PT_KEY")
    AK=os.getenv("AK")
    SK=os.getenv("SK")
    AWS_REGION=os.getenv("AWS_REGION")
    if AK =="" or SK== "":
        DEST = DEST.replace("s3:", "s3a:")

    CONCURRENCY=os.getenv("CONCURRENCY")
    querys = list()
    for i in range(0, int(CONCURRENCY)):
        query_id = export_partition(conn, DB_NAME, TABLE_NAME, DEST, PT_KEY,
                                 pt_name, AWS_REGION,AK, SK)
        querys.append(query_id)
        time.sleep(1)


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