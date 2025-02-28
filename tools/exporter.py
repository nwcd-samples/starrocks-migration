import time
from datetime import datetime
from collections import deque
import queue
import json
import os
import boto3
import threading
import shutil
from .sparkexporter import run as sparkrun, get_spark
from .sparkexporter import runone as sparkrunone
from .mysql import get_conn
from .log import get_logger
from .helper import get_tasks, send_task_done_notification, split_task_filter

logger = get_logger("exporter")


class EWorkerThread(threading.Thread):
    """
    增量桶的迁移
    """

    def __init__(self, job_name, table_name, deque_queue, msg_queue, index):
        threading.Thread.__init__(self)
        self.job_name = job_name
        self.table_name = table_name
        self.index = index
        self.deque_queue = deque_queue
        self.msg_queue = msg_queue

    def run(self):
        spark = get_spark(self.job_name, self.table_name, self.index)
        while True:
            try:
                # 从队列中获取数据
                partition = self.deque_queue.popleft()
                msg = sparkrun(spark, self.job_name, self.table_name, partition, logger)
                logger.warn(f"====>send msg {msg}")
                self.msg_queue.put(msg)
            except IndexError:
                # 如果队列为空，退出线程
                logger.info(f"[exporter][{self.job_name}]===>Thread {self.index}: No more data to process. Exiting.")
                break


class UploadThread(threading.Thread):
    def __init__(self, job_name, table_name, bucket, message_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.job_name = job_name
        self.table_name = table_name
        self.bucket = bucket
        self.msg_queue = message_queue

    def run(self):
        s3_client = boto3.client('s3')
        success_count = 0
        failed_count = 0
        while True:
            msg = self.msg_queue.get()
            file_path, s3_key = msg
            if file_path == "done":
                logger.info(f"[exporter][{self.job_name}]===>{msg} partition exported all !")
                time.sleep(2)
                shutil.rmtree(s3_key)
            else:
                try:
                    s3_client.upload_file(file_path, self.bucket, s3_key)
                    success_count += 1
                    logger.info(f"[exporter][{self.job_name}]===>{success_count} success upload file: {file_path}!")
                except Exception as ex:
                    failed_count += 1
                    logger.error(f"[exporter][{self.job_name}]===>{failed_count} failed to upload file: {file_path}!")


class CheckFileThread(threading.Thread):
    def __init__(self, job_name, table_name, bucket, prefix, message_queue, s3msg_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.job_name = job_name
        self.table_name = table_name
        self.bucket = bucket
        self.prefix = prefix
        self.msg_queue = message_queue
        self.s3msg_queue = s3msg_queue

    def run(self):

        db_name = os.getenv("SOURCE_DB_NAME")
        temp = os.getenv("SPARK_TEMP")
        s3path = f"{self.prefix}/{self.job_name}/{db_name}/{self.table_name}"

        while True:
            msg = self.msg_queue.get()
            if msg == "stop":
                return

            current_files = set()
            for root, _, files in os.walk(msg):
                for file in files:

                    if root.find("_temporary") >= 0:
                        continue
                    if file.startswith("."):
                        continue
                    if not file.endswith("parquet"):
                        continue
                    current_files.add(os.path.join(root, file))

            for file_path in current_files:
                logger.info(f"[exporter][{self.job_name}]===>Begin to scan foler {file_path}")
                s3_path = os.path.relpath(file_path, temp)
                # 构建 S3 对象键
                s3_key = f"{self.prefix}/{s3_path}"
                logger.info(f"[exporter][{self.job_name}]===>Begin to upload file: {file_path}\n{s3_key}!")
                self.s3msg_queue.put((file_path, s3_key))

            self.s3msg_queue.put(("done", msg))

            time.sleep(1)


def run(job_name: str):
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    logger.info(f"NEW　JOB BEGIN {job_name}")
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    storages = os.getenv("STORAGES").split(",")
    dest = storages[0]
    num_threads = int(os.getenv("EXPORT_CONCURRENCY"))
    num2_threads = int(os.getenv("UPLOAD_CONCURRENCY"))

    table_names = os.getenv("TABLE_NAME").split(",")
    data_filters = os.getenv("DATA_FILTER").split(",")

    bucket_info = dest.split("/")
    print(bucket_info)
    s3_bucket = bucket_info[2]
    s3_prefix = f"{bucket_info[3]}"

    task_filters = split_task_filter(os.getenv("TASK_FILTER"))
    task_filters_count = len(task_filters)
    for item_index in range(0, len(table_names)):
        logger.info("")
        logger.info("")
        table_name = table_names[item_index]
        logger.info(f"[exporter][{job_name}]===>BEGIN RUN {table_name}!")
        if item_index < task_filters_count:
            partitions = get_tasks(table_name, task_filters[item_index])
        else:
            partitions = get_tasks(table_name, task_filters[item_index])
        logger.info(partitions)

        check_queue = queue.Queue()
        s3_queue = queue.Queue()

        checkfile = CheckFileThread(job_name, table_name, s3_bucket, s3_prefix, check_queue, s3_queue)
        checkfile.start()

        for i in range(0, num2_threads):
            s3up = UploadThread(job_name, table_name, s3_bucket, s3_queue)
            s3up.start()

        if len(partitions) == 0:
            local_path = sparkrunone(job_name, table_name, logger)
            check_queue.put(local_path)
            time.sleep(1)
        else:
            threads = list()
            data_queue = deque(partitions)
            for index in range(num_threads):
                thread = EWorkerThread(job_name, table_name, data_queue, check_queue, index)
                threads.append(thread)
                thread.start()

            # 等待所有线程完成
            for thread in threads:
                thread.join()

        while True:
            remaining_messages = s3_queue.qsize()
            if remaining_messages == 0:
                break
            time.sleep(5)

        time.sleep(10)

        num_import_threads = int(os.getenv("IMPORT_CONCURRENCY"))
        for i in range(0, num_import_threads):
            send_task_done_notification(job_name)
        logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")

    logger.info(f"[exporter][{job_name}]===>{job_name} bingo!")
