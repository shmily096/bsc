-- Hive SQL
-- Function： 汇率主数据 （DWD 层）
-- History: 
-- 2021-05-23    Donny   v1.0    init

drop table if exists dwd_dim_exchange_rate;
create external table dwd_dim_exchange_rate
(
    from_currency   string
    ,to_currency    string
    ,valid_from     string
    ,rate           string
    ,ratio_from     string
    ,ratio_to       string
) comment '汇率主数据'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_exchange_rate/'
tblproperties ("parquet.compression"="lzo");