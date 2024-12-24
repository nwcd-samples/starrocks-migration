from tools import conf, log, mysql
import os

conf.load_env(".env")
tb = os.getenv("TABLE_NAME")

logger = log.get_logger("test")

cmd_partition = f"SHOW PARTITIONS FROM sungrow.{tb};"

conn = mysql.get_conn()

partitions = list()
stoarge_volume = 0
with conn.cursor() as cursor:
    sql = str(cmd_partition)
    cursor.execute(sql)
    conn.commit()
    rows = cursor.fetchall()
  
    for row in rows:
        if int(row['RowCount']) <= 0:
            continue
        
        if row['DataSize'].endswith("GB"):

            stoarge_volume +=float(row['DataSize'].replace("GB",""))
        logger.info(f"{row['PartitionName']}==>{row['RowCount']}:{row['DataSize']}")
            

conn.close()
logger.info(f"total size is {stoarge_volume} GB")