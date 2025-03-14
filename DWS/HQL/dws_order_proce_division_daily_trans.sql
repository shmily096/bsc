-- Hive SQL
-- Function： division维度看订单处理leadtime
-- 按PGI日期进行分区存储，每个分区存储的是当前已PGI的所有订单处理信息
-- 行数据表示PGI日期下一个batch,material对应的订单处理信息
-- History: 
-- 2021-06-17    Donny   v1.0    init

drop table if exists dws_order_proce_division_daily_trans;
create external table dws_order_proce_division_daily_trans
(
    so_no                string comment 'SO Number'
    ,material            string comment 'SKU'
    ,batch               string comment 'Batch'
    ,division_id         string comment 'BU'
    ,so_dn_datetime      string comment 'so dn create' 
    ,actual_gi_date      string comment 'PGI'
    ,so_create_datetime  string comment 'so create'
    ,order_processing    float comment '订单处理leadtime' 
) comment '订单处理信息'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_order_proce_division_daily_trans/'
tblproperties ("parquet.compression"="lzo");

