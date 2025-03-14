-- Hive SQL
-- Function： LE主数据 （ODS 层）
-- History: 
-- 2021-07-15    Donny   v1.0    init

drop table if exists ods_le_srr;
create external table ods_le_srr
(
    division                string comment 'Division',
    `year`                  string comment 'year',
    `month`                 string comment 'month',
    le_cny                  decimal(18,4) comment 'LE CNY',
    le_usd                  decimal(18,4) comment 'LE USD',
    srr_cny                 decimal(18,4) comment 'SRR CNY',
    srr_usd                 decimal(18,4) comment 'SRR USD',
    srr_version             int comment 'SRR Version'
) comment 'LE and SRR '
partitioned by (dt string)
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_le_srr/';