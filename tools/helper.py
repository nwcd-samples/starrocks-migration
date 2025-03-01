import os
import boto3
import json
from .mysql import get_conn


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
    index2 = partition_str.find("))")
    return partition_str[index1 + 2:index2]


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
