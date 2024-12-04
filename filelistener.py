import json
import time
import urllib.parse
import boto3
from datetime import datetime, timedelta

region_name = "eu-central-1"


# ===========================需要修改或确认的配置参数================================
dynamodb_task_trace_tb = 'starrocks-migration-task-trace-event'
sqs_url = "https://sqs.eu-central-1.amazonaws.com/515491257789/starrocks_migration_queue"
# ===========================需要修改或确认的配置参数================================

FILE_STATUS_UPLOAD_TO_S3 = "file uploaded to s3"

def lambda_handler(event, context):
    print("Received event: " + str(event))
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")

    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]
    task_count = 0
    try:
        dynamodb = boto3.client('dynamodb', region_name=region_name)
        sqs = boto3.client('sqs', region_name=region_name)
        for key in keys:
            if not key.endswith(".csv"):
                continue
            # 格式为 bucket_name/前缀路径(配置文件中配置)/task_name/db_name/table_name/partition_name/file_name.csv
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            task_name = "s3://" + ukey
            
            task_count += 1
            try:
                rep = dynamodb.update_item(
                    TableName=dynamodb_task_trace_tb,
                    Key={
                        'task_name': {'S': task_name}
                    },
                    AttributeUpdates={
                        'status': {
                            'Value':  {
                                "S": f"{FILE_STATUS_UPLOAD_TO_S3}"
                            }
                        },
                        'update_time': {
                            'Value':  {
                                "S": f"{current_time}"
                            }
                        }
                    }
                )
                print(rep)
            except Exception as ex:
                print(f"failed to update dynamodb due to {ex}")
            k_info = {
                "task_name": task_name,
                "update_time": current_time,
                "status": f"{FILE_STATUS_UPLOAD_TO_S3}"
            }

            str_info = json.dumps(k_info)

            sqs.send_message(
                QueueUrl=sqs_url,
                MessageBody=str_info,
                DelaySeconds=0
            )
    except Exception as e:
        return {
            "status": str(e)
        }

    return {
        "status": "ok"
    }