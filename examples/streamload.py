import subprocess
from datetime import datetime, timedelta
import setting
import logging
from logging.handlers import RotatingFileHandler
import time

logger = logging.getLogger(__name__)

# 设置日志级别
logger.setLevel(logging.INFO)
# 创建一个handler，用于写入日志文件
handler = RotatingFileHandler('logs/streamload2.log', maxBytes=100000, backupCount=3)
logger.addHandler(handler)

# 创建一个handler，用于将日志输出到控制台
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

# 定义日志格式
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

def run(task_id, table_name):
    # 定义curl命令
    curl_command = f"""
    curl --location-trusted -u root:demo1234 -H "label:sunpro_{task_id}" \
        -H "Expect:100-continue" \
        -H "column_separator:|" \
        -H "columns: ps_id, ps_key,receive_time, minutes, rowkey, dev_type, pday, msg_time,point_val,insert_time" \
        -T /home/ec2-user/workspace/mockdata/data_point_val_part_{task_id}.csv -XPUT \
        http://{setting.HOST}:8030/api/sungorwpro/{table_name}/_stream_load
    """

    # 使用subprocess运行curl命令
    result = subprocess.run(curl_command, shell=True, text=True, capture_output=True)

    logger.warning(result.stdout)

time.sleep(6000)
for i in range(305, 1000):
    try:
        start_time = datetime.now()
        print(f"begin task {i}\n")
        run(i, "data_point_val")
        end_time = datetime.now()

        # 计算函数执行所花费的时间
        execution_time = end_time - start_time

        # 打印执行时间
        print(f"task {i} 执行cost:{execution_time}")
    except Exception as ex:
        logger.error(ex)
    time.sleep(50)