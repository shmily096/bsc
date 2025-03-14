-- Hive SQL
-- Function： 汇率主数据 （ODS 层）
-- History: 
-- 2021-05-20    Donny   v1.0    draft

drop table if exists ods_exchange_rate;
create external table ods_exchange_rate
(
    from_currency   string
    ,to_currency    string
    ,valid_from     string
    ,rate           string
    ,ratio_from     string
    ,ratio_to       string
) comment '汇率主数据'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_exchange_rate/';