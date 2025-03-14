-- hive sql
-- function： plant维度发货处理leadtime
-- 按pgi日期年月进行分区存储,每个分区存储的是当前已pgi的所有发货处理leadtime
-- 行数据表示plant维度发货处理leadtime
-- history: 
-- 2021-06-27    amanda   v1.0    init

drop table if exists dws_plant_delivery_processing_daily_trans;
create external table dws_plant_delivery_processing_daily_trans
(
    pick_up_plant      string comment 'pick up plant'
    ,pgi_processing    float comment 'pgi process'
    ,actual_gi_date    string comment 'actual gi date'
    ,so_no        string
    ,material   string
    ,batch      string
) comment 'plant维度发货处理leadtime'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_plant_delivery_processing_daily_trans/'
tblproperties ("parquet.compression"="lzo");