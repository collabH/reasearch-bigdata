-- 开启分区非严格模式
set hive.exec.dynamic.partition.mode=nonstrict;
-- 开启动态分区
set hive.exec.dynamic.partition=true;

drop table if exists wh_dwd.dwd_base_event_log;
CREATE EXTERNAL TABLE wh_dwd.dwd_base_event_log
(
    `mid_id`       string,
    `user_id`      string,
    `version_code` string,
    `version_name` string,
    `lang`         string,
    `source`       string,
    `os`           string,
    `area`         string,
    `model`        string,
    `brand`        string,
    `sdk_version`  string,
    `gmail`        string,
    `height_width` string,
    `app_time`     string,
    `network`      string,
    `lng`          string,
    `lat`          string,
    `event_name`   string,
    `event_json`   string,
    `server_time`  string
)
    PARTITIONED BY (`dt` string)
    stored as parquet;

insert overwrite table wh_dwd.dwd_base_event_log
    PARTITION (ds)
select mid_id,
       user_id,
       version_code,
       version_name,
       lang,
       source,
       os,
       area,
       model,
       brand,
       sdk_version,
       gmail,
       height_width,
       app_time,
       network,
       lng,
       lat,
       tmp_k.event_name,
       tmp_k.event_json,
       server_time,
       sdk_log.ds
from (
         select split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[0]  as mid_id,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[1]  as user_id,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[2]  as version_code,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[3]  as version_name,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[4]  as lang,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[5]  as source,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[6]  as os,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[7]  as area,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[8]  as model,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[9]  as brand,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[10] as sdk_version,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[11] as gmail,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[12] as height_width,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[13] as app_time,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[14] as network,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[15] as lng,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[16] as lat,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[17] as ops,
                split(baseField(line, 'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'), '\t')[18] as server_time,
                ds
         from wh_ods.ods_event_log where baseField(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la')<>''
     ) sdk_log lateral view eventJson(ops) tmp_k as event_name, event_json;

