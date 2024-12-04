
from tools import conf
from tools import exporter, importer, validation
from tools.sync import Sync
import argparse
import os



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
    parser_retry.add_argument("--type", type=str, help="重试的类型：import 或者 export , 默认 import", default="import")
    parser_retry.add_argument("--env", type=str, help="配置文件地址", default=".env")

    parser_va = subparsers.add_parser("validation", help="验证")
    parser_va.add_argument("--env", type=str, help="配置文件地址", default=".env")

    args = parser.parse_args()
    if args.command == "export":
        env_path = args.env
        job_name = args.job
        conf.load_env(env_path)
        table_name_str = os.getenv("TABLE_NAMES")
        table_names = table_name_str.split(",")
        for table_name in table_names:
            exporter.run(job_name, table_name)
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
        conf.load_env(env_path)
        importer.retry_failed(job_name)

    elif args.command == "validation":
        env_path = args.env
        conf.load_env(env_path)
        validation.run()
    else:
        parser.print_help()

main()