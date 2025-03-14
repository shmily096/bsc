-- Hive SQL
-- Function： plant维度发货处理leadtime
-- 按PGI日期年月进行分区存储，每个分区存储的是当前已PGI的所有发货处理leadtime
-- 行数据表示plant维度发货处理leadtime
-- History: 
-- 2021-06-27    Amanda   v1.0    init

drop table if exists dwt_plant_delivery_processing_topic;
create external table dwt_plant_delivery_processing_topic
(
    pick_up_plant      string comment
    ,PGI_proce_median  float comment
    ,pgi_month         string comment
    ,pgi_year          string comment 
) comment 'plant维度发货处理leadtime'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_plant_delivery_processing_topic/'
tblproperties ("parquet.compression"="lzo");