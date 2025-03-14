-- Hive SQL
-- Function： plant主数据 （ODS 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_plant_master;
create external table ods_plant_master
(
    plant_code        string,
    search_term2      string,
    search_term1      string,
    postl_code        string,
    city              string,
    name2             string,
    name1             string
) comment 'Plant主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_plant_master/';
