import os
import json

# 定义一个函数来加载.env文件
def load_env(filename='.env'):
    # 确保.env文件存在
    if not os.path.exists(filename):
        raise FileNotFoundError(f"The specified file {filename} was not found.")

    # 读取.env文件中的每行
    with open(filename, 'r') as file:
        for line in file:
            # 忽略注释和空行
            line = line.strip()
            if line and not line.startswith('#'):
                # 分割键和值
                key_value_pair = line.split('=', 1)
                if len(key_value_pair) == 2:
                    key, value = key_value_pair
                    key=key.strip()
                    value=value.strip()
                    # 将键值对设置为环境变量
                    os.environ[key] = value

def get_env(key:str, default_v:str):
    return os.getenv(key, default_v)