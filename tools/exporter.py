import time
from datetime import datetime
from collections import deque
import json
import os
import boto3
from .sparkexporter import run as sparkrun
from .sparkexporter import runone as sparkrunone
from .mysql import get_conn
from .log import get_logger
from .helper import pick_list_key, pick_range_key, get_tasks



logger = get_logger("exporter")

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
    logger.info("")
    logger.info("")
    logger.info(f"[exporter][{job_name}]===>BEGION RUN {table_name}!")
    partitions = get_tasks(table_name)
    logger.info(partitions)
    if len(partitions) > 0:
        sparkrun(job_name,table_name,partitions,logger)
    else:
        sparkrunone(job_name,table_name,logger)
    
    logger.info(f"[exporter][{job_name}]===>ALL EXPORT TASK IN {table_name} DONE !!! bingo!")
