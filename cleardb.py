
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

def delete_all_items(table_name, region_name='eu-central-1'):
    """
    删除 DynamoDB 表中所有数据

    :param table_name: 表名
    :param region_name: AWS 区域
    """
    # 创建 DynamoDB 客户端
    dynamodb = boto3.client('dynamodb', region_name=region_name)

    try:
        # 扫描表中的所有数据
        response = dynamodb.scan(
            TableName=table_name,
            Select='ALL_ATTRIBUTES'
        )

        # 获取所有数据项
        items = response.get('Items', [])
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(
                TableName=table_name,
                Select='ALL_ATTRIBUTES',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        if not items:
            print(f"表 {table_name} 中没有数据。")
            return

        # 构造批量删除请求
        delete_requests = []
        for item in items:
            # 提取主键
            if "task_name" in item:
                delete_requests.append({
                    'DeleteRequest': {
                        'Key':{'task_name':{'S': item['task_name']['S']}}
                    }
                })

        # 每次最多删除 25 条记录
        batch_size = 25
        for i in range(0, len(delete_requests), batch_size):
            batch = delete_requests[i:i + batch_size]
            dynamodb.batch_write_item(
                RequestItems={
                    table_name: batch
                }
            )

        print(f"已删除表 {table_name} 中的所有数据。")

    except NoCredentialsError:
        print("未找到 AWS 凭证。请配置 AWS 凭证。")
    except PartialCredentialsError:
        print("AWS 凭证不完整。请检查配置。")
    except Exception as e:
        print(f"发生错误: {e}")

# 示例调用

if __name__ == "__main__":
    table_name = "starrocks-migration-task-trace-event"  # 替换为你的表名
    delete_all_items(table_name)