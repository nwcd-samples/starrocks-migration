# starrocks-migration
## 导出
python3 app.py export --env .env_ex_au
## 导入
python3 app.py export --env .env_im_au

## 同步 从集群A 到集群B
开发中...
python3 app.py sync --env .env


## Tip
###  for starrocks 3.1
导出只支持export ，并且只支持csv格式