import random
import time
import logging
from logging.handlers import RotatingFileHandler
import threading
import queue
import boto3
import os
import uuid
from .mysql import get_conn


if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
handler = RotatingFileHandler('logs/import.log', maxBytes=100000, backupCount=3)
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
        AK=os.getenv("AK")
        SK=os.getenv("SK")
        AWS_REGION=os.getenv("AWS_REGION")
        while True:
            try:
                task = self.task_queue.get(timeout=3)
           
                sleep_time = random.uniform(0.01, 1.0)
                time.sleep(sleep_time)
                import_task(conn, DB_NAME, TABLE_NAME, task, AWS_REGION,AK, SK)
                self.task_queue.task_done()
            except queue.Empty:
                # 如果队列为空，跳出循环
                conn.close()
                break



def import_task(conn, db_name,table_name, file_path: str,aws_region:str,ak="",sk=""):
    # 生成一个UUID（版本4）
    uuid_v4 = uuid.uuid4()

    # 将UUID转换为字符串并去除连字符
    uuid_str = uuid_v4.hex
    label = f"{table_name}_{uuid_str}"

    if ak=="" and sk=="":
        command = f"""
                LOAD LABEL {db_name}.{label}
                (
                    DATA INFILE("{file_path}")
                    INTO TABLE {table_name}
                    COLUMNS TERMINATED BY "|"
                )
                WITH BROKER
                PROPERTIES
                (
                    "timeout" = "3600"
                );
                """
    else:
        command = f"""
                LOAD LABEL {db_name}.{label}
                (
                    DATA INFILE("{file_path}")
                    INTO TABLE {table_name}
                    COLUMNS TERMINATED BY "|"
                )
                WITH BROKER
                PROPERTIES
                (
                    "timeout" = "3600",
                    "aws.s3.access_key" = "{ak}",
                    "aws.s3.secret_key" = "{sk}"
        
                );
                """  

    logger.info(f"begin import label:{label} {file_path} to {table_name}")
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()

        # check status
        status_command=f"""
        SELECT STATE FROM information_schema.loads WHERE LABEL = '{label}';
        """
        while True:
            time.sleep(2)
            with conn.cursor() as cursor:
                cursor.execute(status_command)
                conn.commit()
                row = cursor.fetchone()
                status = row['STATE'] 

                if status == 'FINISHED':
                    logger.info(f"success to import {file_path} to {table_name}")
                    break
                elif status == 'CANCELLED':
                    logger.info(f"failed to import {file_path} to {table_name}")
                    break
                else:
                    time.sleep(5)

            

    except Exception as ex:
        logger.info(f"failed to import {file_path} to {table_name}")
        logger.error(ex)
        logger.error(command)



def parse_s3_path(s3_path):
    # 移除路径前面的's3://'
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    # 按照'/'分割路径
    parts = s3_path.split('/', 1)
    # 存储桶名称是第一部分
    bucket_name = parts[0]
    # 前缀是第二部分，如果存在的话
    prefix = parts[1] if len(parts) > 1 else ''
    return bucket_name, prefix

def get_tasks():
    AK=os.getenv("AK")
    SK=os.getenv("SK")
    AWS_REGION=os.getenv("AWS_REGION")
    SOURCE=os.getenv("SOURCE")
    bucket_name, prefix = parse_s3_path(SOURCE)

    # 
    protocal = "s3"
    if AK !="" and SK !="":
        s3 = boto3.client('s3',
                    aws_access_key_id=AK,
                    aws_secret_access_key=SK,
                    region_name=AWS_REGION)
    else:
        protocal = "s3a"
        s3 = boto3.client('s3',
                  region_name=AWS_REGION)

    

    # 初始化ContinuationToken
    continuationToken = None
    objects = []
    # 循环直到没有更多的对象
    while True:
        if continuationToken:
            # 如果有ContinuationToken，则在请求中使用它
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, ContinuationToken=continuationToken)
        else:
            # 否则，进行第一次请求
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # 提取对象键
        if 'Contents' in response:
            for obj in response['Contents']:
                objects.append(f"{protocal}://{bucket_name}/{obj['Key']}")

        # 检查是否还有更多对象
        if response.get('IsTruncated', False):
            # 如果结果被截断，使用NextContinuationToken进行下一次请求
            continuationToken = response['NextContinuationToken']
        else:
            # 如果没有更多对象，退出循环
            break

    return objects


def run(with_condition=False):
    CONCURRENCY=os.getenv("CONCURRENCY")

    task_queue = queue.Queue()  # 创建任务队列
    # 向队列中添加任务
    tasks = get_tasks()
    
    logger.info(f"the number of task is {len(tasks)}")
    for task in tasks:
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
