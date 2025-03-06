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

    def __init__(self, job_name, deque_queue, msg_queue, index):
        threading.Thread.__init__(self)
        self.job_name = job_name
        self.index = index
        self.deque_queue = deque_queue
        self.msg_queue = msg_queue

    def run(self):
        spark = get_spark(self.job_name, self.index)
        while True:
            try:
                # 从队列中获取数据
                table_name, partition = self.deque_queue.get()
                if table_name == "task_done_a0" and not partition:
                    logger.info(
                        f"[exporter][{self.job_name}]===>Thread {self.index}: No more data to process. Exiting.")
                    break

                if partition:
                    msg = sparkrun(spark, self.job_name, table_name, partition, logger)
                else:
                    msg = sparkrunone(spark, self.job_name, table_name, logger)
                self.msg_queue.put(msg)
            except IndexError:
                # 如果队列为空，退出线程
                logger.info(f"[exporter][{self.job_name}]===>Thread {self.index}: No more data to process. Exiting.")
                break


class UploadThread(threading.Thread):
    def __init__(self, job_name, bucket, message_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.job_name = job_name
        self.bucket = bucket
        self.msg_queue = message_queue
        self.success_count = 0
        self.failed_count = 0

    def run(self):
        s3_client = boto3.client('s3')

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
                    self.success_count += 1
                except Exception as ex:
                    self.failed_count += 1
                    logger.error(
                        f"[exporter][{self.job_name}]===>{self.failed_count} failed to upload file: {file_path} with error {ex}!")


class CheckFileThread(threading.Thread):
    def __init__(self, job_name, bucket, prefix, message_queue, s3msg_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.job_name = job_name
        self.bucket = bucket
        self.prefix = prefix
        self.msg_queue = message_queue
        self.s3msg_queue = s3msg_queue
        self.count = 0

    def run(self):

        db_name = os.getenv("SOURCE_DB_NAME")
        temp = os.getenv("SPARK_TEMP")

        while True:
            msg = self.msg_queue.get()
            if msg == "":
                # 当前分区无数据
                continue
            if msg == "stop":
                return

            logger.info(f"[exporter][{self.job_name}]===>Begin to scan folder {msg}")
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
                s3_path = os.path.relpath(file_path, temp)
                # 构建 S3 对象键
                s3_key = f"{self.prefix}/{s3_path}"
                self.s3msg_queue.put((file_path, s3_key))
                self.count += 1
            # 发送消息，告知某个文件夹已经完全扫描，上传完以后，可以删除这个文件夹
            self.s3msg_queue.put(("done", msg))

            time.sleep(1)


def run(job_name: str):
    logger.info("")
    logger.info("")
    logger.info("")
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    logger.info(f"NEW JOB BEGIN {job_name}")
    logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")

    storages = os.getenv("STORAGES").split(",")
    dest = storages[0]
    num_threads = int(os.getenv("EXPORT_CONCURRENCY"))
    num2_threads = int(os.getenv("UPLOAD_CONCURRENCY"))

    table_names = os.getenv("TABLE_NAME").split(",")

    bucket_info = dest.split("/")
    s3_bucket = bucket_info[2]
    s3_prefix = f"{bucket_info[3]}"

    task_filters = split_task_filter(os.getenv("TASK_FILTER"))
    task_filters_count = len(task_filters)

    total_success = 0
    total_failed = 0
    total_files = 0

    check_queue = queue.Queue()
    s3_queue = queue.Queue()
    task_queue = queue.Queue()

    checkfile = CheckFileThread(job_name, s3_bucket, s3_prefix, check_queue, s3_queue)
    checkfile.start()

    upload_threads = list()
    for i in range(0, num2_threads):
        s3up = UploadThread(job_name, s3_bucket, s3_queue)
        upload_threads.append(s3up)
        s3up.start()

    threads = list()
    for index in range(num_threads):
        thread = EWorkerThread(job_name, task_queue, check_queue, index)
        threads.append(thread)
        thread.start()

    for item_index in range(0, len(table_names)):
        logger.info("")
        logger.info("")
        table_name = table_names[item_index]
        logger.info(f"[exporter][{job_name}]===>BEGIN RUN {table_name}!")
        if item_index < task_filters_count:
            partitions = get_tasks(table_name, task_filters[item_index])
        else:
            partitions = get_tasks(table_name)
        logger.info(partitions)

        if len(partitions) == 0:
            task_queue.put((table_name, {}))
        else:
            for partition in partitions:
                task_queue.put((table_name, partition))

    # 等待所有线程完成
    for thread in threads:
        task_queue.put(("task_done_a0", {}))
        thread.join()

    check_empty = 0
    while True:
        remaining_messages = s3_queue.qsize()
        if remaining_messages == 0:
            check_empty += 1
        else:
            check_empty = 0
        # 连续60次都是空，才认为队列完全清空了
        if check_empty > 60:
            break
        time.sleep(1)

    time.sleep(10)

    for upload in upload_threads:
        total_success += upload.success_count
        total_failed += upload.failed_count

    total_files += checkfile.count

    logger.info(f"[exporter][{job_name}]===>expected to upload total files  {total_files} files")
    if total_failed > 0:
        logger.warn(f"[exporter][{job_name}]===> upload {total_success} files, failed {total_failed}!")
    else:
        logger.info(f"[exporter][{job_name}]===>upload {total_success} files, failed {total_failed}!")
    num_import_threads = int(os.getenv("IMPORT_CONCURRENCY"))
    send_task_done_notification(job_name, num_import_threads)
    logger.info(f"[exporter][{job_name}]===>{job_name} bingo!")
