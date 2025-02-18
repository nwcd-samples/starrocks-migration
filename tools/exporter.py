import time
from datetime import datetime
from collections import deque
import json
import os
import boto3
from .sparkexporter import run as sparkrun
from .mysql import get_conn
from .log import get_logger
from .helper import send_task_done_notification



logger = get_logger("exporter")

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



def get_tasks(table_name:str)->list:
    # filter method可以有如下类型：EACH,STARTWITH,ENDWITH,RANGE
    task_filter = os.getenv("TASK_FILTER", "")
    select_p = None
    begin = None
    end = None
    if task_filter:
        if task_filter.startswith("EACH("):
            parts = task_filter[len("EACH("):-1].split(",")
            select_p = {item:True for item in parts}
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

            if (select_p and  row["PartitionName"] in select_p) or not select_p:
                if "Range" in row:
                    valuestr = row["Range"]
                    datatype = "str"
                    if valuestr.find("INT") > 0:
                        datatype = "number"

                    p_start, p_end = pick_range_key(valuestr)
                    partitions.append(
                        {
                            "name": row["PartitionName"],
                            "key": row["PartitionKey"],
                            "start": p_start,
                            "end": p_end,
                            "type": datatype,
                            "ptype":"range"
                        }
                    )
                else:
                    valuestr = row["List"]
                    datatype = "str"
                    if valuestr.find("INT") > 0:
                        datatype = "number"

                    p_start= pick_list_key(valuestr)
                    partitions.append(
                        {
                            "name": row["PartitionName"],
                            "key": row["PartitionKey"],
                            "start": p_start,
                            "end": "",
                            "type": datatype,
                            "ptype": "list"
                        }
                    )


                
    conn.close()
    return partitions

def cat():
    time.sleep(5)
    logger.info("begin============>")

def run(job_name:str, table_name:str, partition_name = ""):
    DB_NAME = os.getenv("SOURCE_DB_NAME")
    STORAGES = os.getenv("STORAGES").split(",")
    AK = os.getenv("AK")
    SK = os.getenv("SK")
    AWS_REGION = os.getenv("AWS_REGION")

    dest = STORAGES[0]

    CONCURRENCY = int(os.getenv("EXPORT_CONCURRENCY"))

    logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}!")
    partitions = get_tasks(table_name)
    logger.info(partitions)
    sparkrun(job_name,table_name,partitions,logger)
        
    time.sleep(30)
    send_task_done_notification(job_name)
    logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")
