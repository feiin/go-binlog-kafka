# go-binlog-kafka
A MySQL simple tool for syncing BinLog to Kafka with JSON format 


一个将binlog解析为json并push到kafka消息队列简单工具

## Getting Started

最简单方式启动查看

```
./go-binlog-kafka -src_db_user=root -src_db_pass=123123 -src_db_host=10.0.0.1  -src_db_port=3306 --binlog_timeout=100  -src_db_gtid=68414ab6-fd2a-11ed-9e2d-0242ac110002:1 -db_instance_name="test" -debug=true

``` 

## Options

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