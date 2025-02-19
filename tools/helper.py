import os
import boto3
import json

def send_task_done_notification(job_name:str):
    aws_region = os.getenv("AWS_REGION")
    k_info = {
        "task_name": "ALL TASK DONE",
        "update_time": "",
        "status": job_name
    }

    # 告诉immporter ,导出任务结束了
    str_info = json.dumps(k_info)
    queue_url = os.getenv("TASK_QUEUE")
    queue_endpoint = os.getenv("TASK_QUEUE_ENDPOINT")
    sqs = boto3.client('sqs', region_name=aws_region, endpoint_url=f"https://{queue_endpoint}")
    importer_count = int(os.getenv("IMPORT_CONCURRENCY"))
    for i in range(0, importer_count):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=str_info,
            DelaySeconds=0
        )

def pick_range_key(partition_range_str):
    # 首先，根据"keys: ["分割字符串，然后取第二部分
    parts = partition_range_str.split("keys: [")
    key1_str = parts[1].split("];")[0]
    key2_str = parts[2].split("];")[0]
    return key1_str, key2_str

def pick_list_key(partition_str:str):
    index1 = partition_str.find("((")
    index2 = partition_str.find("))")
    return partition_str[index1+2:index2]
