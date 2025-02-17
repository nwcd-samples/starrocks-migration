#!/bin/bash

# 检查是否传入了参数
if [ -z "$1" ]; then
    echo "Error: No job name provided. Usage: $0 <job_name>"
    exit 1
fi

# 获取传入的参数作为 job_name
JOB_NAME="$1"

# 构造 spark-submit 命令
spark-submit \
    --jars /home/ec2-user/lib/hadoop-aws-3.3.4.jar,/home/ec2-user/lib/aws-java-sdk-bundle-1.12.367.jar,/home/ec2-user/lib/starrocks-spark-connector-3.4_2.12-1.1.2.jar,/home/ec2-user/lib/mysql-connector-java-8.0.22.jar \
    taskmanager.py \
    export \
    --env .env \
    --job "$JOB_NAME"