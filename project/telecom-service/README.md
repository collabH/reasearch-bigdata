# 电信客服项目架构

## 生产数据(ProduceLog)
* 随机生成电话话
* 随机生成通话建立时间
* 随机生成通话时长
* 生成日志写入文件

## FLume
* Taildir
* diskchannel
* kafkasink

## Kafka

* 消费数据存储HBase

## MR

* 读取HBase数据，定义指标输出到Mysql中

# 数据结构
## HBase数据结构
* call1 call1_name  call2 call2_name date_time date_time_ts duration