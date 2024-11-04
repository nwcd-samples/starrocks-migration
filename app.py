
from tools import conf
from tools import exporter, importer
import argparse



def main():
    # 创建一个解析器
    parser = argparse.ArgumentParser(description="Starrocks 集群同步")
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    # 创建 parser_t'';l'klk,.  emplate 子命令的解析器
    parser_export = subparsers.add_parser("export", help="导出")
    parser_export.add_argument("--env", type=str, help="配置文件地址", default=".env_ex")
    
    # 创建 print 子命令的解析器
    parser_sync = subparsers.add_parser("sync", help="根据插入键导出增量数据")
    parser_sync.add_argument("--env", type=str, help="配置文件地址",  default=".env_ex")

    parser_import = subparsers.add_parser("import", help="导入")
    parser_import.add_argument("--env", type=str, help="配置文件地址", default=".env_im")

    args = parser.parse_args()
    if args.command == "export":
        env_path = args.env
        conf.load_env(env_path)
        exporter.run()
    elif args.command == "sync":
        env_path = args.env
        conf.load_env(env_path)
        exporter.run(with_condition=True)
    elif args.command == "import":
        env_path = args.env
        conf.load_env(env_path)
        importer.run()
    else:
        parser.print_help()


main()
