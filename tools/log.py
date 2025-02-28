import logging
from logging.handlers import RotatingFileHandler

import os


def get_logger(logger_name: str):
    if not os.path.exists('logs'):
        os.makedirs('logs')

    logger = logging.getLogger(logger_name)

    # 设置日志级别
    logger.setLevel(logging.INFO)
    # 创建一个handler，用于写入日志文件
    handler = RotatingFileHandler(f'logs/{logger_name}.log', maxBytes=1000000, backupCount=100)
    logger.addHandler(handler)

    # 创建一个handler，用于将日志输出到控制台
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)

    # 定义日志格式
    formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
    handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    return logger
