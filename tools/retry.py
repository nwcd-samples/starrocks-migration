import os
import boto3
from boto3.dynamodb.conditions import Key, Attr
import time
from datetime import datetime
import json
from .helper import send_task_done_notification
from enum import Enum

FILE_STATUS_RETRY = "IMPORTED RETRY"


# 定义一个枚举类
class RetryAction(Enum):
    IMPORT_TASK = 1
    IMPORT_PARTITIONS = 2
    EXPORT = 3
    SYNC = 4
    SHOW = 5


def get_retry_action(desc: str):
    if desc == "import":
        pass


class RetryFactory:
    def __init__(self, job_name: str):
        self.job_name = job_name

    def run(self, action: RetryAction, force=False, **kwargs):
        if action == RetryAction.IMPORT_TASK:
            if force:
                items = self._get_job_import_tasks()
                print(items)
            else:
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
        aws_region = os.getenv("AWS_REGION")
        queue_url = os.getenv("TASK_QUEUE")
        sqs = boto3.client('sqs', region_name=aws_region)

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
        send_task_done_notification(self.job_name, num_import_threads)

    def _get_failed_import_tasks(self):
        storages = os.getenv("STORAGES").split(",")
        storage = storages[-1]

        key_prefix_str = os.path.join(storage, self.job_name)
        filter_str = "IMPORTED SUCCESSFULLY"
        return self._get_records(key_prefix_str, filter_str)

    def _get_job_import_tasks(self):
        storages = os.getenv("STORAGES").split(",")
        storage = storages[-1]

        key_prefix_str = os.path.join(storage, self.job_name)

        filter_str = ""
        return self._get_records(key_prefix_str, filter_str)

    def _get_failed_import_partitions(self, partition_name: str):
        storages = os.getenv("STORAGES").split(",")
        storage = storages[-1]
        db_name = os.getenv("SOURCE_DB_NAME")
        tb_name = os.getenv("TABLE_NAME")

        # 格式为 s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv

        key_prefix_str = os.path.join(storage, self.job_name, db_name, tb_name, partition_name)

        filter_str = ""
        return self._get_records(key_prefix_str, filter_str)

    def _get_failed_export_tasks(self):
        STORAGES = os.getenv("STORAGES").split(",")
        storage = STORAGES[-1]

        key_prefix_str = f"{storage}/{self.job_name}"
        filter = "IMPORTED SUCESSFULLY"
        pass

    def _get_records(self, prefix: str, filter_str: str):

        aws_region = os.getenv("AWS_REGION")
        recoder = os.getenv("RECORDER")

        # 初始化boto3的DynamoDB服务客户端
        dynamodb = boto3.resource('dynamodb', region_name=aws_region)  # 替换为你的区域

        # 指定你的DynamoDB表
        table = dynamodb.Table(recoder)

        # 计算总的段数，这取决于你的表的大小和需求
        total_segments = 10  # 例如，你可以设置为10

        # 扫描操作
        def scan_table(segment, total_segments, key_prefix):
            scan_kwargs = {
                'Segment': segment,
                'TotalSegments': total_segments
            }
            if filter_str == "":
                scan_kwargs['FilterExpression'] = Key('task_name').begins_with(key_prefix)
            else:
                scan_kwargs['FilterExpression'] = Key('task_name').begins_with(key_prefix) & Attr('status').ne(
                    filter_str)

            response = table.scan(**scan_kwargs)
            return response

        # 并行执行扫描
        results = []
        for segment in range(0, total_segments):
            results.append(scan_table(segment, total_segments, prefix))

        # 合并结果
        all_items = [item["task_name"] for result in results for item in result.get('Items', [])]
        return all_items
