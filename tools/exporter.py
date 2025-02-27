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
from .helper import pick_list_key, pick_range_key, get_tasks, send_task_done_notification



logger = get_logger("exporter")


class EWorkerThread(threading.Thread):
    """
    增量桶的迁移
    """
    def __init__(self, job_name,table_name, deque_queue, msg_queue, index):
        threading.Thread.__init__(self)
        self.job_name = job_name
        self.table_name = table_name
        self.index = index
        self.deque_queue=deque_queue
        self.msg_queue = msg_queue

    def run(self):
        spark = get_spark(self.job_name, self.table_name,self.index)
        while True:
            try:
                # 从队列中获取数据
                partition = self.deque_queue.popleft()
                msg = sparkrun(spark, self.job_name, self.table_name, partition, logger)
                self.msg_queue.put(msg)
            except IndexError:
                # 如果队列为空，退出线程
                print(f"Thread {self.index}: No more data to process. Exiting.")
                break

class WorkeUploadThread(threading.Thread):
    def __init__(self, job_name,table_name,bucket,message_queue): 
        threading.Thread.__init__(self)
        self.daemon = True
        self.job_name = job_name
        self.table_name = table_name
        self.bucket= bucket
        self.msg_queue = message_queue

    def run(self):
        s3_client = boto3.client('s3')
        success_count=0
        failed_count = 0
        while True:
            msg = self.msg_queue.get()
            file_path, s3_key =msg
            if file_path == "done":
                logger.info(f"[exporter][{self.job_name}]===>{msg} parition exported all !")
                time.sleep(2)
                shutil.rmtree(s3_key)
            else:
                try:
                    s3_client.upload_file(file_path,self.bucket , s3_key)
                    success_count+=1
                    logger.info(f"[exporter][{self.job_name}]===>{success_count} success upload file: {file_path}!")
                except Exception as ex:
                    failed_count+=1
                    logger.error(f"[exporter][{self.job_name}]===>{failed_count} failed to upload file: {file_path}!")


 
class WorkerCheckFileThread(threading.Thread):
    def __init__(self, job_name,table_name, bucket, prefix,message_queue, s3msg_queue):
        threading.Thread.__init__(self)
        self.daemon = True
        self.job_name = job_name
        self.table_name = table_name
        self.bucket = bucket
        self.prefix = prefix
        self.msg_queue = message_queue
        self.s3msg_queue = s3msg_queue



    def run(self):
        
        db_name= os.getenv("SOURCE_DB_NAME")
        temp= os.getenv("SPARK_TEMP")
        s3path = f"{self.prefix}/{self.job_name}/{db_name}/{self.table_name}"
        
        while True:
            msg = self.msg_queue.get()
            if msg == "stop":
                return
            
            current_files = set()
            for root, _, files in os.walk(msg):
                for file in files:
                    
                    if root.find("_temporary") >=0:
                        continue
                    if file.startswith("."):
                        continue
                    if not file.endswith("parquet"):
                        continue
                    current_files.add(os.path.join(root, file))
            
            
            for file_path in current_files:
                logger.warn(file_path)
                s3_path = os.path.relpath(file_path, temp)
                    # 构建 S3 对象键
                s3_key = f"{self.prefix}/{s3_path}"
                logger.info(f"[exporter][{self.job_name}]===>begin to upload file: {file_path}\n{s3_key}!")
                self.s3msg_queue.put((file_path,s3_key))

            self.s3msg_queue.put(("done",msg))
            
                
            time.sleep(1)
            



def run(job_name:str, table_names:list, partition_name = ""):
    DB_NAME = os.getenv("SOURCE_DB_NAME")
    STORAGES = os.getenv("STORAGES").split(",")
    AK = os.getenv("AK")
    SK = os.getenv("SK")
    AWS_REGION = os.getenv("AWS_REGION")
    num_threads = int(os.getenv("EXPORT_CONCURRENCY"))
    num2_threads = int(os.getenv("UPLOAD_CONCURRENCY"))

    dest = STORAGES[0]
    logger.info("")
    logger.info("")

    bucket_info = dest.split("/")
    print(bucket_info)
    s3_bucket = bucket_info[2]
    s3_prefix = f"{bucket_info[3]}"

    for table_name in table_names:
        logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}!")
        partitions = get_tasks(table_name)
        logger.info(partitions)

        message_queue = queue.Queue()
        s3_queue = queue.Queue()

        
        checkfile = WorkerCheckFileThread(job_name, table_name, s3_bucket,s3_prefix,message_queue, s3_queue)
        checkfile.start()

        for i in range(0, num2_threads):
            s3up = WorkeUploadThread(job_name, table_name, s3_bucket,s3_queue)
            s3up.start()


        if len(partitions) == 0:
            sparkrunone(job_name,table_name,logger)
        else:
            threads = list()
            data_queue = deque(partitions)
            for index in range(num_threads):
                thread = EWorkerThread(job_name, table_name, data_queue, message_queue, index)
                threads.append(thread)
                thread.start()

            # 等待所有线程完成
            for thread in threads:
                thread.join()

        while True:
            remaining_messages = s3_queue.qsize()
            if remaining_messages>0:
                time.sleep(5)
        time.sleep(60)


        num_import_threads = int(os.getenv("IMPORT_CONCURRENCY"))
        for i in range(0, num_import_threads):
            send_task_done_notification(job_name)
    logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")
