import os
from .mysql import get_conn
from .log import get_logger
from .helper import pick_list_key, pick_range_key


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


def run():
    logger = get_logger("validation")
    table_name_str = os.getenv("TABLE_NAME")
    metric_str = os.getenv("TABLE_METRICS")
    table_names = table_name_str.split(",")
    metrics = metric_str.split(",")

    tablecal = dict()
    for i in range(0, len(table_names)):
        metric_items = metrics[i].split("|")
        tablecal[table_names[i]] = metric_items


    notmatch = []

    for table_name in table_names:
        logger.info(f"====================>begin analyze the table {table_name}")
        conn = get_conn()
        dest = get_conn(cluster_type="target")

        partitions = get_tasks(table_name)
        for partition in partitions:
            name =f"{table_name}_{partition['name']}"
            pt_name=partition['name']
            tb_metric_items = tablecal[table_name]
            tb_metric_items_str =",".join([f"sum({item}) as {item}" for item in tb_metric_items])
            cmd = f"""select "{name}" as name,{tb_metric_items_str}, count(*) as row_count from {table_name} partition({pt_name})"""
            source_values = dict()
            target_values = dict()
            try:
                with conn.cursor() as cursor:
                    cursor.execute(cmd)
                    conn.commit()
                    rows = cursor.fetchall()
                    for row in rows:
                        
                        compare_values = [str(row["row_count"])]
                        for metric in tb_metric_items:
                            compare_values.append(str(row[metric]))
                        source_values[row["name"]] = ",".join(compare_values)
            except Exception as ex:
                logger.error(f"SOURCE : {cmd}")
                logger.error(ex)
        
            try:
                with dest.cursor() as cursor:
                    cursor.execute(cmd)
                    dest.commit()
                    rows = cursor.fetchall()
                    for row in rows:
                        compare_values = [str(row["row_count"])]
                        for metric in tb_metric_items:
                            compare_values.append(str(row[metric]))
                        target_values[row["name"]] = ",".join(compare_values)
            except Exception as ex:
                logger.error(f"TARGET : {cmd}")
                logger.error(ex)
            

            for key in source_values:
                if source_values[key] !=target_values[key]:
                    logger.error(f"NOT MATCH {key}==>{source_values[key] } != {target_values[key]}")
                    notmatch.append(partition['name'])
                else:
                    logger.info(f"MATCH {key}==>{source_values[key] } == {target_values[key]}")

                

        conn.close()
        dest.close()
    if len(notmatch) > 0:
        logger.error(f"NOT MATCH {','.join(notmatch)}")
    else:
        logger.info("ALL PARTITION IS IS RIGHT !!!")
            