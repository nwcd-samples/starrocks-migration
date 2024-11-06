CREATE TABLE `data_point_val` (
  `ps_id` varchar(16) NOT NULL COMMENT "电站id",
  `ps_key` varchar(64) NOT NULL COMMENT "设备pskey",
  `receive_time` datetime NOT NULL COMMENT "数据当地时区5分钟规整后的时间",
  `minutes` int(11) NOT NULL COMMENT "分钟数",
  `rowkey` varchar(64) NOT NULL COMMENT "原Hbase的rowkey不带hash",
  `dev_type` int(11) NULL COMMENT "设备类型",
  `pday` varchar(8) NOT NULL COMMENT "日期",
  `msg_time` datetime NULL COMMENT "设备时间",
  `point_val` json NULL COMMENT "",
  `insert_time` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT "插入数据时间东八区",
  INDEX idx_ps_key (`ps_key`) USING BITMAP COMMENT 'ps_key的索引',
  INDEX idx_receive_time (`receive_time`) USING BITMAP COMMENT 'receive_time的索引',
  INDEX idx_minutes (`minutes`) USING BITMAP COMMENT 'minutes的索引',
  INDEX idx_ps_id (`ps_id`) USING BITMAP COMMENT 'ps_id的索引',
  INDEX idx_dev_type (`dev_type`) USING BITMAP COMMENT 'dev_type的索引',
  INDEX index_rowkey (`rowkey`) USING BITMAP COMMENT 'rowkey的索引'
) ENGINE=OLAP 
UNIQUE KEY(`ps_id`, `ps_key`, `receive_time`, `minutes`, `rowkey`, `dev_type`, `pday`)
COMMENT "olap"
PARTITION BY date_trunc('day', receive_time)
DISTRIBUTED BY HASH(`ps_id`) BUCKETS 256 
PROPERTIES (
"replication_num" = "1",
"bloom_filter_columns" = "ps_key, minutes, ps_id, receive_time, rowkey, dev_type",
"datacache.partition_duration" = "730 days",
"datacache.enable" = "true",
"enable_async_write_back" = "false",
"enable_persistent_index" = "false",
"compression" = "LZ4"
);
