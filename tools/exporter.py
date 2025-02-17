import time
from datetime import datetime
from collections import deque
import json
import os
import boto3
import sparkexporter
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



def get_summit_task(conn, db_name):
    query_ids = list()
    cmd1 = f"""SHOW EXPORT FROM {db_name} WHERE STATE='PENDING'"""
    cmd2 = f"""SHOW EXPORT FROM {db_name} WHERE STATE='EXPORTING'"""
    for cmd in [cmd1, cmd2]:
        with conn.cursor() as cursor:
            cursor.execute(cmd)
            conn.commit()
            rows = cursor.fetchall()
            for row in rows:
                query_ids.append(row['QueryId'])
    
    return query_ids

def check_task(conn, db_name, query_ids: list) -> dict:
    status = dict()
    running_jobs_count = 0

    for query_id_str in query_ids:
        JOB_CMD1 = f"""SHOW EXPORT FROM {db_name} WHERE QUERYID='{query_id_str}'"""
        with conn.cursor() as cursor:
            cursor.execute(JOB_CMD1)
            conn.commit()
            rows = cursor.fetchall()
            # PENDING：表示查询待调度的导出作业。
            # EXPORTING：表示查询正在执行中的导出作业。
            # FINISHED：表示查询成功完成的导出作业。
            # CANCELLED：表示查询失败的导出作业。
            for row in rows:
                job_id = row['JobId']
                query_id = row['QueryId']
                task_str = row['TaskInfo']
                error_msg= row['ErrorMsg']
                task_info = json.loads(task_str)
                partition = task_info['partitions'][0]
                tb = task_info['tbl']
                summary = f"{job_id}_{query_id}_{tb}_{partition}"
                state = row['State']
                if state == 'PENDING' or state == 'EXPORTING':
                    running_jobs_count += 1

                
                status[query_id] = {
                        'state': state,
                        "summary":summary,
                        "msg": error_msg
                    }
             
    return running_jobs_count, status


def export_partition(conn, job_name:str, db_name:str, table_name:str, dest: str, pt_name: str, aws_region: str, ak: str,
                     sk: str):
    dest.endswith("/")
    # s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
    # 例如 s3://tx-au-mock-data/sunexf/test1/sunim/data_point_val/p20231103/data_01add602-b21d-11ef-b192-0ac76da15273_0_1_0_2_0.csv
    path = dest + f"{job_name}/{db_name}/{table_name}/{pt_name}/" if dest.endswith("/") else f"{dest}/{job_name}/{db_name}/{table_name}/{pt_name}/"
    s3_endpoint = os.getenv("S3_INTERFACE_ENDPOINT")

    if ak != "" and sk != "":
        command = f"""
            EXPORT TABLE {db_name}.{table_name} 
            PARTITION ({pt_name})
            TO "{path}" 
            PROPERTIES
            (
                "column_separator"="|#",
                "timeout" = "3600"
            )
            WITH BROKER
            (
                "aws.s3.access_key" = "{ak}",
                "aws.s3.secret_key" = "{sk}",
                "aws.s3.endpoint" = "https://bucket.{s3_endpoint}",
                "aws.s3.region" = "{aws_region}"
            )
            """
    else:
        command = f"""
            EXPORT TABLE {db_name}.{table_name} 
            PARTITION ({pt_name})
            TO "{path}" 
            PROPERTIES
            (
                "column_separator"="|#",
                "timeout" = "3600"
            )
            WITH BROKER
            (
                "aws.s3.endpoint" = "https://bucket.{s3_endpoint}",
                "aws.s3.region" = "{aws_region}"
            )
            """
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()

        logger.info(f"[exporter][{job_name}]===>success to commit task {pt_name}")

        # JOB_CMD = "SELECT LAST_QUERY_ID() as result"
        # with conn.cursor() as cursor:
        #     cursor.execute(JOB_CMD)
        #     conn.commit()
        #     t = cursor.fetchone()
        #     return t['result']

    except Exception as ex:
        logger.error(f"[exporter][{job_name}]===>failed to export {pt_name}")
        logger.error(f"[exporter][{job_name}]===>{ex}")




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
                ptype ="range" if "Range" in row else "list"
                if "Range" in row:
                    valuestr = row["Range"]
                    datatype = "str"
                    if valuestr.find("INT") > 0:
                        datatype = "number"

                    start, end = pick_range_key(valuestr)
                    partitions.append(
                        {
                            "name": row["PartitionName"],
                            "key": row["PartitionKey"],
                            "ptype": ptype,
                            "start": start,
                            "end": end,
                            "type": datatype,
                            "ptype":"range"
                        }
                    )
                else:
                    valuestr = row["List"]
                    datatype = "str"
                    if valuestr.find("INT") > 0:
                        datatype = "number"

                    start= pick_list_key(valuestr)
                    partitions.append(
                        {
                            "name": row["PartitionName"],
                            "key": row["PartitionKey"],
                            "ptype": ptype,
                            "start": start,
                            "end": "",
                            "type": datatype,
                            "ptype": "list"
                        }
                    )


                
    conn.close()
    return partitions


def run(job_name:str, table_name:str, partition_name = ""):
    DB_NAME = os.getenv("SOURCE_DB_NAME")
    STORAGES = os.getenv("STORAGES").split(",")
    AK = os.getenv("AK")
    SK = os.getenv("SK")
    AWS_REGION = os.getenv("AWS_REGION")

    dest = STORAGES[0]

    CONCURRENCY = int(os.getenv("EXPORT_CONCURRENCY"))

    # 向队列中添加任务
    partitions = get_tasks(table_name)
    for partition in partitions:
        sparkexporter.run(table_name)
        
    logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}!")

    time.sleep(60)
    send_task_done_notification()
    logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")
