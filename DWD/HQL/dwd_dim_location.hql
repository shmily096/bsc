-- Hive SQL
-- Function： Location维度表
-- History: 
-- 2021-05-07    Donny   v1.0    draft

drop table if exists dwd_dim_locaiton;
create external table dwd_dim_locaiton
(
    d_plant           string,
    plant_name        string,
    location_id       string,
    location_status   string,
    storage_location  string,
    storage_definition string
) COMMENT '库位维度信息'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_locaiton/'
tblproperties ("parquet.compression"="lzo");