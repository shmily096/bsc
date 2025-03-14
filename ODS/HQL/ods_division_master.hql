-- Hive SQL
-- Function： BU主数据 （ODS 层）
-- History: 
-- 2021-05-20    Donny   v1.0    draft

drop table if exists ods_division_master;
create external table ods_division_master
(
    id          string
    ,division   string
    ,short_name string
    ,cn_name    string
    ,display_name string
) comment 'BU主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_division_master/';