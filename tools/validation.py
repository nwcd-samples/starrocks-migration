import os
from .mysql import get_conn
from .log import get_logger



def pick_key(partition_range_str):
    # 首先，根据"keys: ["分割字符串，然后取第二部分
    parts = partition_range_str.split("keys: [")
    key1_str = parts[1].split("];")[0]
    key2_str = parts[2].split("];")[0]
    return key1_str, key2_str

def get_tasks(conn, table_name:str)->list:
    # TASK_FILTER = os.getenv("TASK_FILTER", "")
    # if TASK_FILTER:
    #     parts = TASK_FILTER.split(",")
    #     partitions = [{"name": pt_name} for pt_name in parts]
    #     return partitions
    

    cmd_partition = f"SHOW PARTITIONS FROM {table_name}"
    partitions = list()
    with conn.cursor() as cursor:
        sql = str(cmd_partition)
        cursor.execute(sql)
        conn.commit()
        rows = cursor.fetchall()
        for row in rows:
            begin, end = pick_key(row["Range"])
            partitions.append(
                {
                    "name": row["PartitionName"],
                    "key": row["PartitionKey"],
                    "begin": begin,
                    "end":end

                }
            )
    return partitions

def run():
    logger = get_logger("validation")
    table_name_str = os.getenv("TABLE_NAME")
    table_names = table_name_str.split(",")
    
    notmatch = []

    for table_name in table_names:
        logger.info(f"====================>begin analyze the table {table_name}")
        conn = get_conn()
        dest = get_conn(cluster_type="target")

        partitions = get_tasks(conn, table_name)
        for partition in partitions:
            name =f"{table_name}_{partition['name']}"
            key = partition['key']
            begin =  partition['begin']
            end =  partition['end']
            cmd = f"""select "{name}" as name, count(*) as row_count from {table_name} where {key} between "{begin}" and "{end}";"""

            source_count = 0
            target_count = 0
            with conn.cursor() as cursor:
                cursor.execute(cmd)
                conn.commit()
                rows = cursor.fetchall()
                for row in rows:
                    pt_name = row["name"]
                    source_count =row["row_count"]
            try:
                with dest.cursor() as cursor:
                    cursor.execute(cmd)
                    dest.commit()
                    rows = cursor.fetchall()
                    for row in rows:
                        pt_name = row["name"]
                        target_count=row["row_count"]
            except Exception as ex:
                logger.error(cmd)
                logger.error(ex)
            

            if source_count != target_count:
                logger.error(f"{name}==>{source_count} != {target_count}")
                notmatch.append(partition['name'])
            else:
                logger.info(f"{name}==>{source_count} == {target_count}")

        conn.close()
        dest.close()

    logger.error(f"not match partitions {notmatch}")
    logger.info("donepr!!!")
            