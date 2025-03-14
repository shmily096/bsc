-- Hive SQL
-- Function： BU 主数据 （DWD 层）
-- History: 
-- 2021-05-23    Donny   v1.0    init
-- 2021-06-23    Donny   v1.1    alter table dwd_dim_division add columns (sub_division_field_name string)

drop table if exists dwd_dim_division;
create external table dwd_dim_division
(
    id                          string
    ,division                   string
    ,short_name                 string
    ,cn_name                    string
    ,display_name               string
    ,sub_division_field_name    string
) comment 'BU主数据'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_division/'
tblproperties ("parquet.compression"="lzo");