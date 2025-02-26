import os
import boto3
from boto3.dynamodb.conditions import Key,Attr
import time
from datetime import datetime
import json
from .helper import send_task_done_notification
from .log import get_logger

FILE_STATUS_RETRY="IMPORTED RETRY"

from enum import Enum

# 定义一个枚举类
class RetryAction(Enum):
    IMPORT_TASK = 1
    IMPORT_PARTITIONS = 2
    EXPORT = 3
    SYNC = 4
    SHOW = 5

def get_retry_action(desc:str):
    if desc == "import":
        pass
        

class RetryFactory:
    def __init__(self, job_name:str):
        self.job_name = job_name

    def help():
        print("Retry Help")
        print("==============================")

    def run(self, action:RetryAction, **kwargs):
        if action == RetryAction.IMPORT_TASK:
            items = self._get_failed_import_tasks()
            self._retry_import_tasks(items)
            return
        
        if action == RetryAction.IMPORT_PARTITIONS:
            if 'partition_name' not in kwargs:
                raise ValueError("Miss param partition_name")
            
            partition_name = kwargs['partition_name']
            items = self._get_failed_import_partitions(partition_name)
            self._retry_import_tasks(items)
            return
        

    def _retry_import_tasks(self, tasks: list):
        AWS_REGION = os.getenv("AWS_REGION")
        queue_url = os.getenv("TASK_QUEUE")
        sqs = boto3.client('sqs', region_name=AWS_REGION)

        now = datetime.now()
        current_time = now.strftime("%Y-%m-%d %H:%M:%S")
        for task in tasks:
            k_info = {
                "task_name": task,
                "update_time": current_time,
                "status": f"{FILE_STATUS_RETRY}"
            }

            str_info = json.dumps(k_info)
            print(f"send new task {str_info}")
            sqs.send_message(
                QueueUrl=queue_url,
                MessageBody=str_info,
                DelaySeconds=0
            )
        num_import_threads = int(os.getenv("IMPORT_CONCURRENCY"))
        time.sleep(10)
        for i in range(0, num_import_threads):
            send_task_done_notification(self.job_name)



    def _get_failed_import_tasks(self):
        STORAGES = os.getenv("STORAGES").split(",")
        storage = STORAGES[-1]

        key_prefix_str=f"{storage}/{self.job_name}"
        filter="IMPORTED SUCESSFULLY"
        return self._get_records(key_prefix_str, filter)

    def _get_failed_import_partitions(self, partition_name:str):
        STORAGES = os.getenv("STORAGES").split(",")
        db_name = os.getenv("SOURCE_DB_NAME")
        tb_name = os.getenv("TABLE_NAME")
        storage = STORAGES[-1]
        # 格式为 s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
        if storage.endswith("/"):
            key_prefix_str=f"{storage}{self.job_name}/{db_name}/{tb_name}/{partition_name}"
        else:
            key_prefix_str=f"{storage}/{self.job_name}/{db_name}/{tb_name}/{partition_name}"
        print(key_prefix_str)
        filter=""
        return self._get_records(key_prefix_str, filter)


    def _get_failed_export_tasks(self):
        STORAGES = os.getenv("STORAGES").split(",")
        storage = STORAGES[-1]

        key_prefix_str=f"{storage}/{self.job_name}"
        filter="IMPORTED SUCESSFULLY"
        pass


    def _get_records(self, prefix:str, filter:str):

        AWS_REGION = os.getenv("AWS_REGION")
        RECORDER = os.getenv("RECORDER")
        
        
        # 初始化boto3的DynamoDB服务客户端
        dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)  # 替换为你的区域

        # 指定你的DynamoDB表
        table = dynamodb.Table(RECORDER)

        # 计算总的段数，这取决于你的表的大小和需求
        total_segments = 10  # 例如，你可以设置为10

        # 扫描操作
        def scan_table(segment, total_segments, key_prefix):
            scan_kwargs = {
                'Segment': segment,
                'TotalSegments': total_segments
            }
            if filter == "":
                scan_kwargs['FilterExpression'] =  Key('task_name').begins_with(key_prefix)
            else:
                scan_kwargs['FilterExpression'] = Key('task_name').begins_with(key_prefix) & Attr('status').ne(filter)


            response = table.scan(**scan_kwargs)
            return response

        # 并行执行扫描
        results = []
        for segment in range(0, total_segments):
            results.append(scan_table(segment, total_segments, prefix))

        # 合并结果
        all_items = [item["task_name"] for result in results for item in result.get('Items', [])]
        return all_items

