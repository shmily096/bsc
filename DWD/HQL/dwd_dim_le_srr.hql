-- Hive SQL
-- Function： LE主数据 （DWD层）
-- History: 
-- 2021-07-15    Donny   v1.0    init

drop table if exists dwd_dim_le_srr;
create external table dwd_dim_le_srr
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
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_le_srr/'
tblproperties ("parquet.compression"="lzo");