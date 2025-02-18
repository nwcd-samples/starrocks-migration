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
    importer_count = int(os.getenv("IMPORT_CONCURRENCY"))
    sqs = boto3.client('sqs', region_name=aws_region)
    for i in range(0, importer_count):
        sqs.send_message(
            QueueUrl=queue_url,
            MessageBody=str_info,
            DelaySeconds=0
        )
