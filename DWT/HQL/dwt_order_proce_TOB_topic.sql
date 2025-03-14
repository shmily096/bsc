-- hive sql
-- function： type of business维度看订单处理leadtime
-- 按pgi年月进行分区存储，每个分区存储的是当前已pgi的所有订单处理信息
-- 行数据表示pgi日期下年月对应的订单处理信息
-- history: 
-- 2021-07-09    amanda   v1.0    init

drop table if exists dwt_order_proce_tob_topic;
create external table dwt_order_proce_tob_topic
(
    item_type                   string  
    ,order_processing_median    float comment '订单处理leadtime' 
    ,pgi_month                  string 
    ,pgi_year                   string 
) comment '订单处理信息'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_order_proce_tob_topic/'
tblproperties ("parquet.compression"="lzo");
