-- Hive SQL
-- Function： 批号主数据 （DWD 层）
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists dwd_dim_batch;
create external table dwd_dim_batch
(
    material          string comment '物料编码',
    batch             string comment '批号',
    shelf_life_exp_date string comment '有效期',
    country_of_origin string comment '产地',
    date_of_manuf     string comment '生产日期',
    cfda              string comment '质量证书编号'
) comment '批号主数据'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_batch/'
tblproperties ("parquet.compression"="lzo");