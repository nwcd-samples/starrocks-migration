from tools import conf
from tools import exporter, importer, validation
from tools.retry import RetryFactory, RetryAction
from tools.helper import db, clear_sqs
from tools.sync import Sync
import argparse
import os


def main():
    # 创建一个解析器
    parser = argparse.ArgumentParser(description="Starrocks 集群同步")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    parser_export = subparsers.add_parser("export", help="导出")
    parser_export.add_argument("--job", type=str, help="启动作业名称")
    parser_export.add_argument("--env", type=str, help="配置文件地址", default=".env")

    # 创建 print 子命令的解析器
    parser_sync = subparsers.add_parser("sync", help="根据插入键导出增量数据")
    parser_sync.add_argument("--job", type=str, help="启动作业名称")
    parser_sync.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_import = subparsers.add_parser("import", help="导入")
    parser_import.add_argument("--job", type=str, help="启动作业名称")
    parser_import.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_retry = subparsers.add_parser("retry", help="重试")
    parser_retry.add_argument("--job", type=str, help="启动作业名称")
    parser_retry.add_argument("--force", type=bool, help="强制从新把指定任务的数据重新加载", default=False)
    parser_retry.add_argument("--type", type=str, help="重试的类型,目前支持如下值: [import export] , 默认 import",
                              default="import")
    parser_retry.add_argument("--content", type=str,
                              help="重试的内容,目前支持如下值:  [files,partition_name] , 默认 files, 即所有失败的文件，partition_name 需要你传入想要重试的partition name ",
                              default="files")
    parser_retry.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_va = subparsers.add_parser("validation", help="验证")
    parser_va.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_sqs = subparsers.add_parser("clear", help="情况SQS")
    parser_sqs.add_argument("--env", type=str, help="配置文件地址", default=".env")
    parser_sqs.add_argument("--job", type=str, help="启动作业名称")

    parser_db = subparsers.add_parser("db", help="清理记录状态的DB")
    parser_db.add_argument("--env", type=str, help="配置文件地址", default=".env")
    parser_db.add_argument("--job", type=str, help="启动作业名称")
    parser_db.add_argument("--type", type=str, help="查询db状态或者删除job 的状态记录: [scan clear] , 默认 scan", default="scan")

    args = parser.parse_args()
    if args.command == "export":
        env_path = args.env
        job_name = args.job
        conf.load_env(env_path)
        exporter.run(job_name)
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
        force = args.force
        content = args.content
        conf.load_env(env_path)
        redo = RetryFactory(job_name)
        if retype == "import":
            if content == "files":
                redo.run(RetryAction.IMPORT_TASK, force=force)
            else:
                redo.run(RetryAction.IMPORT_PARTITIONS, partition_name=content)

    elif args.command == "validation":
        env_path = args.env
        conf.load_env(env_path)
        validation.run()
    elif args.command == "clear":
        env_path = args.env
        conf.load_env(env_path)
        job_name = args.job
        clear_sqs(job_name)
    elif args.command == "db":
        env_path = args.env
        conf.load_env(env_path)
        job_name = args.job
        if args.type == "clear":
            db(job_name, delete=True)
        else:
            db(job_name)
    else:
        parser.print_help()


main()
