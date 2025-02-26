import os
import time
from functools import partial
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import DataFrame
import shutil

class S3Uploader:
    def __init__(self,
                 s3_bucket: str,
                 s3_prefix: str,
                 max_retries: int = 3,
                 retry_delay: int = 30,
                 delete_local: bool = True,
                 polling_interval: int = 5,
                 logger=None):
        """
        S3 文件上传器类
        :param s3_bucket: S3 存储桶名称
        :param s3_prefix: S3 对象键前缀
        :param max_retries: S3 上传最大重试次数
        :param retry_delay: 重试间隔时间（秒）
        :param delete_local: 是否删除本地文件
        :param polling_interval: 目录监控轮询间隔（秒）
        :param logger: 日志记录器
        """
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.delete_local = delete_local
        self.polling_interval = polling_interval
        self.logger = logger or print
        self.processed_files = set()  # 已处理文件集合

    def upload_file_with_key(self, file_path: str, s3_key: str) -> bool:
        """
        上传单个文件到 S3，使用指定的 S3 对象键
        :param file_path: 本地文件路径
        :param s3_key: S3 对象键
        :return: True（成功）/ False（失败）
        """
        if not file_path or not os.path.isfile(file_path):
            self.logger.error(f"Invalid file path: {file_path}")
            return False
        
        for attempt in range(self.max_retries):
            try:
                self.logger.info(f"Uploading {file_path} to s3://{self.s3_bucket}/{s3_key}")
                s3_client = boto3.client('s3')
                s3_client.upload_file(file_path, self.s3_bucket, s3_key)
                return True
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', 'Unknown')
                error_msg = e.response.get('Error', {}).get('Message', 'Unknown')
                if attempt == self.max_retries - 1:
                    self.logger.error(f"Failed to upload {file_path}: {error_code} - {error_msg}")
                    return False
                else:
                    self.logger.warning(f"Upload failed for {file_path}, attempt {attempt+1}/{self.max_retries}: {error_msg}")
                    time.sleep(self.retry_delay)
        
        self.logger.error(f"Failed to upload {file_path} after {self.max_retries} attempts")
        return False

    def watch_and_upload(self, directory: str,s3path:str) -> None:
        """
        持续监控目录中的新文件并上传，保持原有文件结构
        :param directory: 本地目录路径
        """
        self.logger.info(f"[exporter]===>begin check {directory}")
        while True:
            try:
                # 获取当前目录所有文件
                current_files = set()
                for root, _, files in os.walk(directory):
                    for file in files:
                        
                        if root.find("_temporary") >=0:
                            continue
                        if file.startswith("."):
                            continue
                        if not file.endswith("parquet"):
                            continue
                        current_files.add(os.path.join(root, file))
                
                # 检测新文件
                new_files = current_files - self.processed_files
                if not new_files:
                    self.logger.debug("No new files detected")
                    time.sleep(self.polling_interval)
                    continue
                
                # 处理新文件
                
                for file_path in new_files:
                    self.logger.info(f"Processing new file: {file_path}")
                    
                    # 计算文件相对于监控目录的路径
                    relative_path = os.path.relpath(file_path, directory)
                    # 构建 S3 对象键
                    s3_key = f"{s3path}/{relative_path}"
                    
                    # 上传文件到 S3
                    success = self.upload_file_with_key(file_path, s3_key)
                    
                    if success and self.delete_local:
                        try:
                            os.remove(file_path)
                            self.logger.info(f"Deleted local file: {file_path}")
                        except Exception as e:
                            self.logger.error(f"Failed to delete {file_path}: {str(e)}")
                    else:
                        self.logger.warning(f"Failed to delete local file after successful upload: {file_path}")
                    
                    # 更新已处理文件集合
                    self.processed_files.add(file_path)
                
                # 清理空目录
                for root, dirs, _ in os.walk(directory, topdown=False):
                    if not dirs:
                        dir_path = os.path.join(root, '')
                        if dir_path.startswith(directory):
                            try:
                                os.rmdir(dir_path)
                                self.logger.info(f"Removed empty directory: {dir_path}")
                            except Exception as e:
                                self.logger.error(f"Failed to remove directory: {dir_path}")
            
            except Exception as e:
                self.logger.error(f"Unexpected error in directory monitoring: {str(e)}", exc_info=True)
                time.sleep(self.polling_interval)