import os
import boto3
import json
from .mysql import get_conn
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


def send_task_done_notification(job_name: str, count=1):
    aws_region = os.getenv("AWS_REGION")
    k_info = {
        "task_name": "ALL TASK DONE",
        "update_time": "",
        "status": job_name
    }

    # 告诉importer ,导出任务结束了
    str_info = json.dumps(k_info)
    queue_url = os.getenv("TASK_QUEUE")
    queue_endpoint = os.getenv("TASK_QUEUE_ENDPOINT")
    sqs = boto3.client('sqs', region_name=aws_region, endpoint_url=f"https://{queue_endpoint}")
    for i in range(0, count):
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


def pick_list_key(partition_str: str):
    index1 = partition_str.find("((")
    if index1 == 0:
        index2 = partition_str.find("))")
        return partition_str[index1 + 2:index2]
    else:
        return partition_str[1:-1]


def split_task_filter(filter_str: str) -> list:
    # Step 1: 用 `),` 分割字符串
    parts = filter_str.split('),')

    functions = []
    for part in parts:
        # Step 2: 检查并补全右括号
        if part.rstrip() and not part.endswith(')'):
            part += ')'
        # 过滤空字符串（例如输入末尾多出的 `)` 导致的空片段）
        if part.strip():
            functions.append(part)
    return functions


def get_tasks(table_name: str, task_filter: str = "") -> list:
    if task_filter.startswith("ALL"):
        return list()

    # filter method可以有如下类型：DEFAULT,ALL,EACH,STARTWITH,ENDWITH,RANGE
    select_p = None
    begin = None
    end = None
    if task_filter:
        if task_filter.startswith("EACH("):
            parts = task_filter[len("EACH("):-1].split(",")
            select_p = {item: True for item in parts}
        elif task_filter.startswith("RANGE("):
            parts = task_filter[len("RANGE("):-1].split(",")
            begin = parts[0]
            end = parts[-1]
        elif task_filter.startswith("STARTWITH("):
            parts = task_filter[len("STARTWITH("):-1].split(",")
            begin = parts[0]
            end = ""
        elif task_filter.startswith("ENDWITH("):
            parts = task_filter[len("ENDWITH("):-1].split(",")
            begin = ""
            end = parts[-1]

    conn = get_conn()

    cmd_partition = f"SHOW PARTITIONS FROM {table_name} ORDER BY PartitionName"
    partitions = list()
    with conn.cursor() as cursor:
        sql = str(cmd_partition)

        cursor.execute(sql)
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            if begin:
                if row["PartitionName"] < begin:
                    continue

            if end:
                if row["PartitionName"] >= end:
                    break

            if (select_p and row["PartitionName"] in select_p) or not select_p:
                if "Range" in row:
                    value_str = row["Range"]
                    if not value_str:
                        continue
                    datatype = "str"
                    if value_str.find("INT") > 0:
                        datatype = "number"

                    p_start, p_end = pick_range_key(value_str)
                    partitions.append(
                        {
                            "name": row["PartitionName"],
                            "key": row["PartitionKey"],
                            "start": p_start,
                            "end": p_end,
                            "type": datatype,
                            "ptype": "range",
                            "rowcount": int(row["RowCount"])
                        }
                    )
                else:
                    value_str = row["List"]
                    if not value_str:
                        continue
                    datatype = "str"
                    if value_str.find("INT") > 0:
                        datatype = "number"

                    p_start = pick_list_key(value_str)
                    partitions.append(
                        {
                            "name": row["PartitionName"],
                            "key": row["PartitionKey"],
                            "start": p_start,
                            "end": "",
                            "type": datatype,
                            "ptype": "list",
                            "rowcount": int(row["RowCount"])
                        }
                    )

    conn.close()
    return partitions


def clear_sqs(job: str = ""):
    aws_region = os.getenv("AWS_REGION")

    queue_url = os.getenv("TASK_QUEUE")
    sqs = boto3.client('sqs', region_name=aws_region)
    stat = dict()
    while True:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            VisibilityTimeout=1
        )
        if "Messages" not in response:
            break

        messages = response["Messages"]
        if len(messages) == 0:
            break

        for msg in messages:
            body_str = msg["Body"]
            receipt_handle = msg["ReceiptHandle"]
            body = json.loads(body_str)
            task_name = body['task_name']
            if task_name == "ALL TASK DONE":
                item_job_name = body['status']
                if item_job_name not in stat:
                    stat[item_job_name] = 1
                else:
                    stat[item_job_name] += 1
            else:
                parts = task_name.split("/")
                item_job_name = parts[4]

            if not job or item_job_name == job:
                print(f"Clear SQS message in {body_str}")
                sqs.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=receipt_handle
                )
    print(stat)


# 扫描操作
def scan_table(table, segment, total_segments, key_prefix, filter_str=""):
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


def clear_db(job_name: str):
    table_name = os.getenv("RECORDER")
    # 创建 DynamoDB 客户端
    aws_region = os.getenv("AWS_REGION")
    dynamodb = boto3.client('dynamodb', region_name=aws_region)

    # 初始化boto3的DynamoDB服务客户端
    dynamodbs = boto3.resource('dynamodb', region_name=aws_region)  # 替换为你的区域

    # 指定你的DynamoDB表
    table = dynamodbs.Table(table_name)

    storages = os.getenv("STORAGES").split(",")
    storage = storages[-1]

    # 格式为 s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
    if storage.endswith("/"):
        key_prefix_str = f"{storage}{job_name}"
    else:
        key_prefix_str = f"{storage}/{job_name}"
    total_segments = 10
    try:

        # 并行执行扫描
        results = []
        for segment in range(0, total_segments):
            results.append(scan_table(table, segment, total_segments, key_prefix_str))

        # 合并结果

        all_items = [item["task_name"] for result in results for item in result.get('Items', [])]

        # 构造批量删除请求
        delete_requests = []
        for item in all_items:
            print(f"[ClearDB]=======>WILL DELETE {item}")
            delete_requests.append({
                'DeleteRequest': {
                    'Key': {'task_name': {'S': item}}
                }
            })

        # 每次最多删除 25 条记录
        batch_size = 25
        for i in range(0, len(delete_requests), batch_size):
            print("Begin deletes....")
            batch = delete_requests[i:i + batch_size]
            ok = dynamodb.batch_write_item(
                RequestItems={
                    table_name: batch
                }
            )
            print(ok)

        print(f"已删除表 {job_name} {len(all_items)}中的所有数据。")

    except NoCredentialsError:
        print("未找到 AWS 凭证。请配置 AWS 凭证。")
    except PartialCredentialsError:
        print("AWS 凭证不完整。请检查配置。")
    except Exception as e:
        print(f"发生错误: {e}")
