# go-binlog-kafka
A simple MySQL tool for syncing BinLog to Kafka with JSON format 

一个将binlog解析为json并push到kafka消息队列简单工具

## Getting Started

最简单方式启动查看

```
./go-binlog-kafka -src_db_user=root -src_db_pass=123123 -src_db_host=10.0.0.1  -src_db_port=3306 --binlog_timeout=100  -src_db_gtid=68414ab6-fd2a-11ed-9e2d-0242ac110002:1 -db_instance_name="test" -debug=true -kafka_addr=10.0.0.1:9092,10.0.0.2:9092 -kafka_topic_name=test

``` 

## 推送的JSON格式数据

每条数据变更(insert/update/delete)都会解析以下JSON
```
{
    "binlog_file": "mysql-bin.000052", // binlog file
    "log_pos": 3013167, // binlog position
    "action": "insert", // insert/update/delete/DDL action
    "table": "tests",  // 表名称
    "gtid": "68414ab6-fd2a-11ed-9e2d-0242ac110002:1-608",// GTID
    "schema": "tests", // 库名称
    "values": null, // insert/delete 时是对应行数据
    "before_values":{...} // update 变更前行数据
    "after_values":{...} // update 变更后行数据
}
```

```
# insert时，推送格式数据如下
[{"binlog_file":"mysql-bin.000052","log_pos":3013167,"action":"insert","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-608","schema":"tests","values":{"id":8,"name":"insert test","type":1}}.....]

# update时，推送格式如下

[{"binlog_file":"mysql-bin.000052","log_pos":3013722,"action":"update","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-610","schema":"tests","before_values":{"id":8,"name":"insert test","type":1},"after_values":{"id":8,"name":"update test","type":1}}....]

# delete时,推送数据如下
[{"binlog_file":"mysql-bin.000052","log_pos":3014032,"action":"delete","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-611","schema":"tests","values":{"id":8,"name":"update test","type":1}}]

# DDL操作时

[{"binlog_file":"mysql-bin.000052","log_pos":3014315,"action":"DDL","table":"tests","gtid":"68414ab6-fd2a-11ed-9e2d-0242ac110002:1-612","schema":"tests","values":{"ddl_events":[{"schema":"tests","table":"tests","type":"alter_table"}],"ddl_sql":"alter table tests add column sign varchar(32) not null default '' comment 'sign'"}}...]
```

## Options

- kafka_addr 推送目标kafka的地址
- kafka_topic_name 推送目标的kafka topic名称
- src_db_user Binlog源库用户
- src_db_pass Binlog源库密码
- src_db_host Binlog源库Host
- src_db_port Binlog源库端口
- binlog_timeout Binlog的read timeout
- src_db_gtid Binlog源库开始GTID，第一次启动配置起作用， gtid获取方式，在源库上执行`show master status;`可以获取
- db_instance_name 实例名称，同时启动多个时保证唯一值
- meta_store_type 程序解析binlog的位置等meta信息保存方式,默认`file`,可选`mysql`,当选`mysql`时需要配置*admin_*开头选项参数
- admin_host meta保存库host,-meta_store_type=mysql时必填
- admin_port meta保存库端口,-meta_store_type=mysql时必填
- admin_user meta保存库用户,-meta_store_type=mysql时必填
- admin_pass meta保存库密码,-meta_store_type=mysql时必填
- admin_database meta保存库名称，默认`binlog_center`
- debug  开启调试模式时，会输出推送kafka消息详情

### meta 保存表结构信息

当`meta_store_type`值为mysql时，需要在meta保存库上创建以下库和表
```
create database binlog_center;

CREATE TABLE `binlog_info` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `instance_name` varchar(32) DEFAULT NULL COMMENT 'instance name',
  `binlog_file` varchar(32) DEFAULT '' COMMENT 'binlog file',
  `binlog_pos` int(11) DEFAULT '0' COMMENT 'binlog position',
  `binlog_gtid` varchar(256) DEFAULT '' COMMENT 'Executed_Gtid_Set',
  `meta_data` json DEFAULT NULL COMMENT 'table meta data',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT 'create time',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'update time',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_ix` (`instance_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
```

