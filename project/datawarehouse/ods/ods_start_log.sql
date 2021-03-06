drop table if exists wh_ods.ods_start_log;


create external table if not exists wh_ods.ods_start_log
(
    line string
) partitioned by (ds string)
    row format delimited fields terminated by '\t'
    stored as textfile
    tblproperties ('author' = 'hsm','date' = '20200818','desc' = '启动日志表');


load data inpath 'hdfs://hadoop:8020/flume/gmall/log/topic_start1/20-08-19/'
    overwrite into table wh_ods.ods_start_log partition (ds = '2020-08-19')