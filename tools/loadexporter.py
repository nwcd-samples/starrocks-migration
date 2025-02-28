import time
from datetime import datetime
from collections import deque
import json
import os
import boto3
from mysql import get_conn
from log import get_logger
from helper import send_task_done_notification, get_tasks

logger = get_logger("exporter")


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


def check_task(conn, db_name, query_ids: list) -> tuple[int, dict]:
    status = dict()
    running_jobs_count = 0

    for query_id_str in query_ids:
        job_cmd1 = f"""SHOW EXPORT FROM {db_name} WHERE QUERYID='{query_id_str}'"""
        with conn.cursor() as cursor:
            cursor.execute(job_cmd1)
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
                error_msg = row['ErrorMsg']
                task_info = json.loads(task_str)
                partition = task_info['partitions'][0]
                tb = task_info['tbl']
                summary = f"{job_id}_{query_id}_{tb}_{partition}"
                state = row['State']
                if state == 'PENDING' or state == 'EXPORTING':
                    running_jobs_count += 1

                status[query_id] = {
                    'state': state,
                    "summary": summary,
                    "msg": error_msg
                }

    return running_jobs_count, status


def export_partition(conn, job_name: str, db_name: str, table_name: str, dest: str, pt_name: str, aws_region: str,
                     ak: str,
                     sk: str):
    # s3://bucket_name/前缀路径(配置文件中配置)/job_name/db_name/table_name/partition_name/file_name.csv
    # 例如 s3://tx-au-mock-data/sunexf/test1/sunim/data_point_val/p20231103/
    # data_01add602-b21d-11ef-b192-0ac76da15273_0_1_0_2_0.csv
    path = dest + f"{job_name}/{db_name}/{table_name}/{pt_name}/" if dest.endswith(
        "/") else f"{dest}/{job_name}/{db_name}/{table_name}/{pt_name}/"
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


def run(job_name: str, table_name: str, partition_name=""):
    db_name = os.getenv("SOURCE_DB_NAME")
    storages = os.getenv("STORAGES").split(",")
    ak = os.getenv("AK")
    sk = os.getenv("SK")
    aws_region = os.getenv("AWS_REGION")

    dest = storages[0]
    if ak == "" or sk == "":
        dest = dest.replace("s3:", "s3a:")

    concurrency = int(os.getenv("EXPORT_CONCURRENCY"))

    # 向队列中添加任务
    if partition_name:
        partitions = [{
            "name": partition_name
        }]
    else:
        partitions = get_tasks(table_name)

    task_deque = deque(partitions)
    logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}!")
    while len(task_deque) > 0:
        now = datetime.now()
        counter = 0
        conn = get_conn()
        try:
            while counter < concurrency and len(task_deque) > 0:
                task = task_deque.popleft()
                pt_name = task["name"]
                counter += 1
                export_partition(conn, job_name, db_name, table_name, dest,
                                 pt_name, aws_region, ak, sk)

            query_ids = get_summit_task(conn, db_name)
            time.sleep(5)
            running = True
            while running:
                running_jobs_count, status = check_task(conn, db_name, query_ids)
                if running_jobs_count == 0:
                    running = False
                    for key in status:
                        job = status[key]["summary"]
                        if status[key]["state"] == "FINISHED":
                            logger.info(f"[exporter][{job_name}]===>Succeed in exporting job {job}")
                        else:
                            error_msg = status[key]["msg"]
                            logger.error(f"[exporter][{job_name}]===>Failed to finish job {job} due to {error_msg}")
                else:
                    time.sleep(5)
            time.sleep(2)
        except Exception as ex:
            logger.error(f"[exporter][{job_name}]===>Failed to finish job {job_name} due to {ex}, reconnecting...")
            conn.close()
            time.sleep(60)
            conn = get_conn()

    time.sleep(60)
    send_task_done_notification(job_name)
    logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")
