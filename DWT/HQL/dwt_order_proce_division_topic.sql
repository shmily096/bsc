-- Hive SQL
-- Function： division维度看订单处理leadtime
-- 按PGI年月进行分区存储，每个分区存储的是当前已PGI的所有订单处理信息
-- 行数据表示PGI日期下年月对应的订单处理信息
-- History: 
-- 2021-07-09    Amanda   v1.0    init

drop table if exists dwt_order_proce_division_topic;
create external table dwt_order_proce_division_topic
(
    division_id                 string  
    ,order_processing_median    float comment '订单处理leadtime' 
    ,pgi_month                  string 
    ,pgi_year                   string 
) comment '订单处理信息'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_order_proce_division_topic/'
tblproperties ("parquet.compression"="lzo");