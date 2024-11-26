import json
import time
import urllib.parse
import boto3
from datetime import datetime, timedelta

region_name = "cn-northwest-1"
max_parallel_count = 5
time_diff = 10  # 十分钟内不容许上传相同的文档

# ===========================需要修改或确认的配置参数================================
dynamodb_task_trace_tb = 'starrocks-migration-task-trace-event'
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/027040934161/sam-data-plaftform-test"
# ===========================需要修改或确认的配置参数================================

FILE_STATUS_UPLOAD_TO_S3 = "FILE_UPLOAD_TO_S3"
FILE_STATUS_READ_TO_IMPORT = "FILE_READ_TO_IMPORT"
FILE_STATUS_IMPORTED = "FILE_IMPORTED"

def check_key(dynamodb, task_name, file_name, cond):

    response = dynamodb.query(
        TableName=dynamodb_task_trace_tb,
        Limit=100,
        Select='ALL_ATTRIBUTES',
        KeyConditions={
            'task_name': {
                'AttributeValueList': [
                    {
                        'S': task_name,
                    },
                ],
                'ComparisonOperator': 'EQ'
            },
            'file_name': {
                'AttributeValueList': [
                    {
                        'S': file_name,
                    },
                ],
                'ComparisonOperator': 'EQ'
            },
            'event_time': {
                'AttributeValueList': [
                    {
                        'S': cond
                    },
                ],
                'ComparisonOperator': 'GT'
            }
        })
    return len(response['Items'])


def lambda_handler(event, context):
    print("Received event: " + str(event))
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d %H:%M:%S")
    condition = (now - timedelta(minutes=time_diff)
                 ).strftime("%Y-%m-%d %H:%M:%S")

    
    keys = [record['s3']['bucket']['name'] + "/" + record['s3']['object']['key']
            for record in event['Records']]
    task_count = 0
    try:
        dynamodb = boto3.client('dynamodb', region_name=region_name)
        sqs = boto3.client('sqs', region_name=region_name)
        for key in keys:
            # 格式为 bucket_name/前缀路径(配置文件中配置)/task_name/db_name/table_name/partition_name/file_name.csv
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            file_key = "s3://" + ukey

            task_count += 1
            dynamodb.update_item(
                TableName=dynamodb_task_trace_tb,
                Key={
                    'file_key': {'S': file_key},
                    'event_time':  {'S': current_time}

                },
                AttributeUpdates={
                    'status': {
                        'Value':  {
                            "S": f"{FILE_STATUS_UPLOAD_TO_S3}"
                        }
                    }
                }
            )
            k_info = {
                "file_key": file_key,
                "event_time": current_time,
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