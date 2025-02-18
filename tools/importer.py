import random
import time
import threading
import queue
import boto3

from datetime import datetime
import json
import os
import uuid
from .mysql import get_conn
from .log import get_logger
from .helper import send_task_done_notification


logger = get_logger("importer")


FILE_STATUS_IMPORTING = "IMPORTING"
FILE_STATUS_IMPORTED_SUCESS = "IMPORTED SUCESSFULLY"
FILE_STATUS_IMPORTED_FAILED = "IMPORTED FAILED"



class EWorkerThread(threading.Thread):
    """
    存量文件的迁移
    """
    def __init__(self, job_name, task_queue):
        threading.Thread.__init__(self)
        self.job_name=job_name
        self.task_queue = task_queue

    def run(self):
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
                status, msg = import_task(self.job_name, DB_NAME, TABLE_NAME, task, AWS_REGION,AK, SK)
                self.task_queue.task_done()
            except queue.Empty:
                # 如果队列为空，跳出循环
                break

class IWorkerThread(threading.Thread):
    """
    增量桶的迁移
    """
    def __init__(self, job_name):
        threading.Thread.__init__(self)
        self.job_name = job_name

    def run(self):
        
        DB_NAME = os.getenv("TARGET_DB_NAME")
        AK = os.getenv("AK")
        SK = os.getenv("SK")
        AWS_REGION = os.getenv("AWS_REGION")

        recorder = os.getenv("RECORDER")
        dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

        queue_url = os.getenv("TASK_QUEUE")
        sqs = boto3.client('sqs', region_name=AWS_REGION)

        while True:
            try:
                response = sqs.receive_message(
                    QueueUrl=queue_url,
                    VisibilityTimeout=600
                )
                if "Messages" not in response:
                    time.sleep(2)
                    continue

                messages = response["Messages"]
  

                task_info = list()
                for msg in messages:
                    body_str = msg["Body"]
                    body = json.loads(body_str)
                    task_name=body['task_name']

                    if task_name == "ALL TASK DONE":
                        item_job_name =  body['status']
                    else:
                        parts = task_name.split("/")
                        item_job_name=parts[4]
                    
                    receipt_handle = msg["ReceiptHandle"]
                    if item_job_name == self.job_name:
                        sqs.delete_message(
                            QueueUrl=queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        
                        if task_name == "ALL TASK DONE":
                            logger.info(f"[importer][{self.job_name}]===>ALL TASK DONE !!!")
                        else:
                            task_info.append(body)

                now = datetime.now()
                current_time = now.strftime("%Y-%m-%d %H:%M:%S")

                for body in task_info:
                    utask_name = body['task_name']

                    res = dynamodb.update_item(
                        TableName=recorder,
                        Key={
                            'task_name': {'S': task_name}

                        },
                        AttributeUpdates={
                            'status': {
                                'Value': {
                                    "S": FILE_STATUS_IMPORTING
                                }
                            },
                            'update_time': {
                                'Value': {
                                    "S": current_time
                                }
                            }
                        }
                    )
                    time.sleep(1)
                    # 格式为 s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
                    # s3://tx-au-mock-data/sunexf/test2/sunim/data_point_val/p20231105/__starrocks_export_tmp_e8134bc5-b224-11ef-b192-0ac76da15273/data_e8134bc5-b224-11ef-b192-0ac76da15273_0_1_0_0.csv
                    # s3://eu-test-starrocks/eu-test-starrocks-2025021203/sungrow/fact_organization_kpi_year/p2018/
                    parts = utask_name.split("/")
                    table_name=parts[6]
                    file_path = utask_name
                    if AK =="" or SK =="":
                        file_path = utask_name.replace("s3://", "s3a://")
                    is_ok, msg = import_task(self.job_name, DB_NAME, table_name, file_path, AWS_REGION, AK, SK)
                    status = FILE_STATUS_IMPORTED_SUCESS if is_ok else FILE_STATUS_IMPORTED_FAILED
                    dynamodb.update_item(
                        TableName=recorder,
                        Key={
                            'task_name': {'S': utask_name}
                        },
                        AttributeUpdates={
                            'status': {
                                'Value': {
                                    "S": status
                                }
                            },
                            'msg': {
                                'Value': {
                                    "S": msg
                                }
                            }
                        }
                    )
                    sleep_time = random.uniform(0.01, 1.0)
                    time.sleep(sleep_time)
                time.sleep(1)
            except Exception as ex:
                logger.error(f"[importer]===>error {ex}")
                time.sleep(10)



def import_task(job_name, db_name,table_name, file_path: str,aws_region:str,ak="",sk=""):
    # 生成一个UUID（版本4）
    now = datetime.now()
    current_time = now.strftime("%Y_%m_%d_%H_%M_%S")
    uuid_v4 = uuid.uuid4()
    ukey = str(uuid_v4)[-4:-1]
    label = f"{table_name}_{current_time}_{ukey}"

    if ak=="" and sk=="":
        command = f"""
                LOAD LABEL {db_name}.{label}
                (
                    DATA INFILE("{file_path}")
                    INTO TABLE {table_name}
                    COLUMNS TERMINATED BY "|#"
                )
                WITH BROKER
                (
                    "aws.s3.region" = "{aws_region}"
                )
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
                    COLUMNS TERMINATED BY "|#"
                )
                WITH BROKER
                (
                    "aws.s3.access_key" = "{ak}",
                    "aws.s3.secret_key" = "{sk}",
                    "aws.s3.region" = "{aws_region}"
                )
                PROPERTIES
                (
                    "timeout" = "3600"
                );
                """  

    logger.info(f"[importer][{job_name}]===>begin import label:{label} {file_path} to {table_name}")
    conn = get_conn(cluster_type="target")
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()

        time.sleep(2)
        # check status
        status_command=f"""
        SELECT LABEL,STATE,ERROR_MSG FROM information_schema.loads WHERE LABEL = '{label}';
        """
        while True:
            with conn.cursor() as cursor:
                cursor.execute(status_command)
                conn.commit()
                row = cursor.fetchone()
                label = row['LABEL'] 
                status = row['STATE']
                msg = row['ERROR_MSG']
                
                res = f"{label}:{status}==>{msg}"
                if status == 'FINISHED':
                    logger.info(f"[importer][{job_name}]===>Succeed in importing {res}")
                    return True, res
                elif status == 'CANCELLED':
                    logger.error(f"[importer][{job_name}]===>Failed to import {res}")
                    return False, res
                else:
                    time.sleep(2)

            

    except Exception as ex:
        conn.close()
        logger.error(f"[importer][{job_name}]===>failed to import {file_path} to {table_name} due to {ex}")
        return False, str(ex)



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

def get_task():
    AWS_REGION = os.getenv("AWS_REGION")

    recorder = os.getenv("RECORDER")
    print(recorder)
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

    queue_url = os.getenv("TASK_QUEUE")
    sqs = boto3.client('sqs', region_name=AWS_REGION)

    response = sqs.receive_message(
        QueueUrl=queue_url,
        VisibilityTimeout=600
    )
    print(response)
    if "Messages" not in response:
        return
    messages = response["Messages"]
    for msg in messages:
        body_str = msg["Body"]
        body = json.loads(body_str)
        receipt_handle = msg["ReceiptHandle"]
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
    )

    response = dynamodb.get_item(
    TableName=recorder,
     Key={
        'task_name': {'S': 'test'}
    })
    print(response)  

def run(job_name:str, incremental=True):
    CONCURRENCY=int(os.getenv("IMPORT_CONCURRENCY"))
    threads = []

    if incremental == False:
        task_queue = queue.Queue()  # 创建任务队列
        # 向队列中添加任务
        tasks = get_tasks()
        logger.info(f"[importer][{job_name}]===>the number of task is {len(tasks)}")
        for task in tasks:
            task_queue.put(task)

        # 创建并启动线程
        for i in range(0, CONCURRENCY):
            thread = EWorkerThread(task_queue, job_name)
            threads.append(thread)
            thread.start()

        # 等待队列中的所有任务完成
        task_queue.join()
    else:
        for i in range(0, CONCURRENCY):
            thread = IWorkerThread(job_name)
            threads.append(thread)
            thread.start()

            # 所有任务完成后，停止线程
    for thread in threads:
        thread.join()


    logger.info(f"[importer][{job_name}]===>JOB FINISHED")

