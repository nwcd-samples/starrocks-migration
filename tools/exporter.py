import time
from datetime import datetime
from collections import deque
import json
import os
import boto3
import threading
from .sparkexporter import run as sparkrun, get_spark
from .sparkexporter import runone as sparkrunone
from .mysql import get_conn
from .log import get_logger
from .helper import pick_list_key, pick_range_key, get_tasks, send_task_done_notification



logger = get_logger("exporter")


class EWorkerThread(threading.Thread):
    """
    增量桶的迁移
    """
    def __init__(self, job_name,table_name, deque_queue, index):
        threading.Thread.__init__(self)
        self.job_name = job_name
        self.table_name = table_name
        self.index = index
        self.deque_queue=deque_queue

    def run(self):
        spark = get_spark(self.job_name, self.table_name,self.index)
        while True:
            try:
                # 从队列中获取数据
                partition = self.deque_queue.popleft()
                sparkrun(spark, self.job_name, self.table_name, partition, logger)
            except IndexError:
                # 如果队列为空，退出线程
                print(f"Thread {self.index}: No more data to process. Exiting.")
                break
                
 

def cat():
    time.sleep(5)
    logger.info("begin============>")

def run(job_name:str, table_name:str, partition_name = ""):
    DB_NAME = os.getenv("SOURCE_DB_NAME")
    STORAGES = os.getenv("STORAGES").split(",")
    AK = os.getenv("AK")
    SK = os.getenv("SK")
    AWS_REGION = os.getenv("AWS_REGION")
    num_threads = int(os.getenv("EXPORT_CONCURRENCY"))

    dest = STORAGES[0]
    logger.info("")
    logger.info("")
    logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}!")
    partitions = get_tasks(table_name)
    logger.info(partitions)
    if len(partitions) == 0:
        sparkrunone(job_name,table_name,logger)
    else:
        threads = list()
        data_queue = deque(partitions)
        for index in range(num_threads):
            thread = EWorkerThread(job_name, table_name, data_queue, index)
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

    time.sleep(60)
    num_import_threads = int(os.getenv("IMPORT_CONCURRENCY"))
    for i in range(0, num_import_threads):
        send_task_done_notification(job_name)
    logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")
