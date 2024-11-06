import time
from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
from collections import deque
import json
import os
from .mysql import get_conn

if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
handler = RotatingFileHandler(
    'logs/export.log', maxBytes=100000, backupCount=3)
logger.addHandler(handler)

# 创建一个handler，用于将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 定义日志格式
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)


def pick_key(partition_range_str):
    # 首先，根据"keys: ["分割字符串，然后取第二部分
    parts = partition_range_str.split("keys: [")
    key1_str = parts[1].split("];")[0]
    key2_str = parts[2].split("];")[0]
    return key1_str, key2_str


def check_task(conn, db_name, start_time: str, query_info) -> dict:
    JOB_CMD1 = f"SHOW EXPORT FROM {db_name}"
    running_jobs_count = 0
    new_finished_jobs = []
    new_failed_jobs = []

    with conn.cursor() as cursor:
        cursor.execute(JOB_CMD1)
        conn.commit()
        rows = cursor.fetchall()
        # PENDING：表示查询待调度的导出作业。
        # EXPORTING：表示查询正在执行中的导出作业。
        # FINISHED：表示查询成功完成的导出作业。
        # CANCELLED：表示查询失败的导出作业。
        for row in rows:
            ctime = row['CreateTime']
            if ctime < start_time:
                continue
            query_id = row['QueryId']
            task_str = row['TaskInfo']
            task_info = json.loads(task_str)
            partition = task_info['partitions'][0]
            tb = task_info['tbl']
            summary = f"{query_id}_{tb}_{partition}"
            state = row['State']
            if state == 'PENDING' or state == 'EXPORTING':
                running_jobs_count += 1

            if query_id not in query_info:

                query_info[query_id] = {
                    'create_time': ctime,
                    'state': state,
                    'partition': partition,
                    'tb': tb
                }
                if state == 'FINISHED':
                    new_finished_jobs.append(summary)
            else:
                new_state = row['State']
                old_state = query_info[query_id]['state']
                query_info[query_id]['state'] = new_state
                if old_state != new_state:
                    if new_state == 'FINISHED':
                        new_finished_jobs.append(summary)
                    if new_state == 'CANCELLED':
                        new_failed_jobs.append(summary)

        return running_jobs_count, new_finished_jobs, new_failed_jobs


def export_partition(conn, db_name, table_name, dest: str, pt_key: str, pt_name: str, aws_region: str, ak: str,
                     sk: str):
    dest.endswith("/")
    path = dest + pt_name if dest.endswith("/") else f"{dest}/{pt_name}"

    if ak != "" and sk != "":
        command = f"""
            EXPORT TABLE {db_name}.{table_name} 
            PARTITION ({pt_name})
            TO "{path}" 
            PROPERTIES
            (
                "column_separator"="|",
                "timeout" = "3600"
            )
            WITH BROKER
            (
                "aws.s3.access_key" = "{ak}",
                "aws.s3.secret_key" = "{sk}",
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
                "column_separator"="|",
                "timeout" = "3600"
            )
            WITH BROKER
            (
                "aws.s3.region" = "{aws_region}"
            )
            """
    try:
        with conn.cursor() as cursor:
            cursor.execute(command)
            conn.commit()

        logger.info(f"success to commit task {pt_name}")

        # JOB_CMD = "SELECT LAST_QUERY_ID() as result"
        # with conn.cursor() as cursor:
        #     cursor.execute(JOB_CMD)
        #     conn.commit()
        #     t = cursor.fetchone()
        #     return t['result']

    except Exception as ex:
        logger.error(f"failed to export {pt_name}")
        logger.error(ex)


def get_tasks():
    TABLE_NAME = os.getenv("TABLE_NAME")
    PT_KEY = os.getenv("PT_KEY")
    conn = get_conn()

    cmd_partition = f"SHOW PARTITIONS FROM {TABLE_NAME}"
    partitions = list()
    with conn.cursor() as cursor:
        sql = str(cmd_partition)
        cursor.execute(sql)
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            if row['PartitionKey'] != PT_KEY:
                logger.warning(
                    f'Partition {row["PartitionName"]}:{row["PartitionKey"]} not match partition key {PT_KEY}')
                continue

            begin, end = pick_key(row["Range"])
            partitions.append(
                {
                    "name": row["PartitionName"],
                    "begin": begin,
                    "end": end
                }
            )
    conn.close()
    return partitions


def run(with_condition=False):
    DB_NAME = os.getenv("DB_NAME")
    TABLE_NAME = os.getenv("TABLE_NAME")
    DEST = os.getenv("DEST")
    PT_KEY = os.getenv("PT_KEY")
    AK = os.getenv("AK")
    SK = os.getenv("SK")
    AWS_REGION = os.getenv("AWS_REGION")
    if AK == "" or SK == "":
        DEST = DEST.replace("s3:", "s3a:")

    CONCURRENCY = int(os.getenv("CONCURRENCY"))

    # 向队列中添加任务
    partitions = get_tasks()
    task_deque = deque(partitions)

    logger.info("task begin")
    while len(task_deque) > 0:
        job_info = dict()
        now = datetime.now()
        begin_timestr = now.strftime("%Y-%m-%d %H:%M:%S")
        counter = 0
        conn = get_conn()
        while counter < CONCURRENCY:
            task = task_deque.popleft()
            pt_name = task["name"]
            counter += 1
            export_partition(conn, DB_NAME, TABLE_NAME, DEST, PT_KEY,
                             pt_name, AWS_REGION, AK, SK)
            time.sleep(0.2)

        running = True
        while running:
            running_jobs_count, new_finished_jobs, new_failed_jobs = check_task(conn, DB_NAME, begin_timestr, job_info)
            for job in new_finished_jobs:
                logger.info(f"success to finish job {job}")
            for job in new_failed_jobs:
                logger.info(f"failed to finish job {job}")
            if running_jobs_count == 0:
                running = False
            else:
                time.sleep(10)
        time.sleep(1)

    logger.info(f"ALL TASK DONE !!! bingo!")
