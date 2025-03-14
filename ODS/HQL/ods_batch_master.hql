-- Hive SQL
-- Function： 批号主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_batch_master;
create external table ods_batch_master
(
    material          string comment '物料编码',
    batch             string comment '批号',
    shelf_life_exp_date string comment '有效期',
    country_of_origin string comment '产地',
    date_of_manuf     string comment '生产日期',
    cfda              string comment '质量证书编号'
) comment '批号主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_batch_master/';