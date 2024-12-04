import os
import logging
from logging.handlers import RotatingFileHandler
import threading
from . import exporter, importer


if not os.path.exists('logs'):
    os.makedirs('logs')

logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
handler = RotatingFileHandler('logs/sync.log', maxBytes=100000, backupCount=3)
logger.addHandler(handler)

# 创建一个handler，用于将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 定义日志格式
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

class IWorkerThread(threading.Thread):
    """
    增量桶的迁移
    """
    def __init__(self, job_name):
        threading.Thread.__init__(self)
        self.job_name = job_name

    def run(self):
        logger.info(f"Begin to run importer")
        importer.run(self.job_name, incremental=True)




class Sync:
    def __init__(self):
        pass

    def run(self, job_name):
        # start importer
        thread = IWorkerThread(job_name)
        thread.start()

        # start exporter
        table_name_str = os.getenv("TABLE_NAMES")
        table_names = table_name_str.split(",")
        for table_name in table_names:
            logger.info(f"Begin to export table {table_name}")
            exporter.run(job_name, table_name)
            
