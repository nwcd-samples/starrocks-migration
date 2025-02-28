import os
from .mysql import get_conn
from .log import get_logger
from .helper import split_task_filter, get_tasks


def compare(sr_conn, dest_conn, cmd: str, tb_metric_items, logger):
    source_values = dict()
    target_values = dict()
    stat = list()
    try:
        with sr_conn.cursor() as cursor:
            cursor.execute(cmd)
            sr_conn.commit()
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
        with dest_conn.cursor() as cursor:
            cursor.execute(cmd)
            dest_conn.commit()
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
        if source_values[key] != target_values[key]:
            logger.error(f"NOT MATCH {key}==>{source_values[key]} != {target_values[key]}")
            stat.append(key)
        else:
            logger.info(f"MATCH {key}==>{source_values[key]} == {target_values[key]}")

    return stat


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

    not_match = []

    task_filters = split_task_filter(os.getenv("TASK_FILTER"))
    task_filters_count = len(task_filters)
    for item_index in range(0, len(table_names)):
        table_name = table_names[item_index]
        logger.info(f"====================>begin analyze the table {table_name}")
        sconn = get_conn()
        dest = get_conn(cluster_type="target")
        tb_metric_items = tablecal[table_name]
        tb_metric_items_str = ",".join([f"sum({item}) as {item}" for item in tb_metric_items])

        if item_index < task_filters_count:
            partitions = get_tasks(table_name, task_filters[item_index])
        else:
            partitions = get_tasks(table_name, task_filters[item_index])
        if not partitions:
            tb_cmd = f"""select "{table_name}" as name,{tb_metric_items_str}, count(*) as row_count from {table_name}"""
            stat = compare(sconn, dest, tb_cmd, tb_metric_items, logger)
            if stat:
                not_match.append(*stat)
        else:
            for partition in partitions:
                name = f"{table_name}_{partition['name']}"
                pt_name = partition['name']

                pt_cmd = f"""select "{name}" as name,{tb_metric_items_str}, count(*) as row_count from {table_name} partition({pt_name})"""
                stat = compare(sconn, dest, pt_cmd, tb_metric_items, logger)
                if stat:
                    not_match.append(*stat)

        sconn.close()
        dest.close()

    if len(not_match) > 0:
        logger.error(f"NOT MATCH {','.join(not_match)}")
    else:
        logger.info("ALL PARTITION IS IS RIGHT !!!")
