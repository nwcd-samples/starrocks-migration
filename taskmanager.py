
from tools import conf
from tools import exporter, importer, validation
from tools.retry import RetryFactory, RetryAction
from tools.helper import send_task_done_notification
from tools.sync import Sync
import argparse
import os
import time
import boto3


def clear():
    AWS_REGION = os.getenv("AWS_REGION")

    queue_url = os.getenv("TASK_QUEUE")
    sqs = boto3.client('sqs', region_name=AWS_REGION)
    while True:
        response = sqs.receive_message(
                    QueueUrl=queue_url,
                    VisibilityTimeout=1
                )
        if "Messages" not in response:
            break

        messages = response["Messages"]
        if len(messages) == 0:
           break

        for msg in messages:
            body_str = msg["Body"]
            print(body_str)
            receipt_handle = msg["ReceiptHandle"]
            ok = sqs.delete_message(
                        QueueUrl=queue_url,
                        ReceiptHandle=receipt_handle
                    )  
def main():
    # 创建一个解析器
    parser = argparse.ArgumentParser(description="Starrocks 集群同步")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    # 创建 parser_t'';l'klk,.  emplate 子命令的解析器
    parser_export = subparsers.add_parser("export", help="导出")
    parser_export.add_argument("--job", type=str, help="启动作业名称")
    parser_export.add_argument("--env", type=str, help="配置文件地址", default=".env")
    
    # 创建 print 子命令的解析器
    parser_sync = subparsers.add_parser("sync", help="根据插入键导出增量数据")
    parser_sync.add_argument("--job", type=str, help="启动作业名称")
    parser_sync.add_argument("--env", type=str, help="配置文件地址",  default=".env")

    parser_import = subparsers.add_parser("import", help="导入")
    parser_import.add_argument("--job", type=str, help="启动作业名称")
    parser_import.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_retry = subparsers.add_parser("retry", help="重试")
    parser_retry.add_argument("--job", type=str, help="启动作业名称")
    parser_retry.add_argument("--type", type=str, help="重试的类型,目前支持如下值: [import export] , 默认 import", default="import")
    parser_retry.add_argument("--content", type=str, help="重试的内容,目前支持如下值:  [files,partition_name] , 默认 files, 即所有失败的文件，partition_name 需要你传入想要重试的partition name ", default="files")
    parser_retry.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_va = subparsers.add_parser("validation", help="验证")
    parser_va.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_sqs = subparsers.add_parser("clear", help="情况SQS")
    parser_sqs.add_argument("--env", type=str, help="配置文件地址", default=".env")

    args = parser.parse_args()
    if args.command == "export":
        env_path = args.env
        job_name = args.job
        conf.load_env(env_path)
        table_name_str = os.getenv("TABLE_NAME")
        table_names = table_name_str.split(",")
        exporter.run(job_name, table_names)
    elif args.command == "import":
        env_path = args.env
        job_name = args.job
        conf.load_env(env_path)
        importer.run(job_name)
    elif args.command == "sync":
        env_path = args.env
        job_name = args.job
        conf.load_env(env_path)
        syncer = Sync()
        syncer.run(job_name)

    elif args.command == "retry":
        env_path = args.env
        job_name = args.job
        retype = args.type
        content = args.content
        conf.load_env(env_path)
        table_name_str = os.getenv("TABLE_NAME")
        redo = RetryFactory(job_name)
        if retype == "import":
            if content == "files":
                redo.run(RetryAction.IMPORT_TASK)
            else:
                redo.run(RetryAction.IMPORT_PARTITIONS, partition_name = content)
        else:
            exporter.run(job_name, table_name_str, partition_name=content)

    elif args.command == "validation":
        env_path = args.env
        conf.load_env(env_path)
        validation.run()
    elif args.command == "clear":
        env_path = args.env
        conf.load_env(env_path)
        clear()
    else:
        parser.print_help()

main()