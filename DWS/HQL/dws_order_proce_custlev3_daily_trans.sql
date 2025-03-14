-- hive sql
-- function： 产品category维度看订单处理leadtime
-- 按pgi日期进行分区存储,每个分区存储的是当前已pgi的所有订单处理信息
-- 行数据表示pgi日期下一个batch,material对应的订单处理信息
-- history: 
-- 2021-06-17    donny   v1.0    init

drop table if exists dws_order_proce_custlev3_daily_trans;
create external table dws_order_proce_custlev3_daily_trans
(
    so_no                string comment 'sales order number'
    ,material            string comment'sku'
    ,batch               string comment'batch'
    ,customer_level3     string comment 'customer level 3'
    ,so_dn_datetime      string comment 'so dn create' 
    ,actual_gi_date      string comment 'pgi'
    ,so_create_datetime  string comment 'so create'
    ,order_processing    float comment '订单处理leadtime' 
) comment '订单处理信息'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_order_proce_custlev3_daily_trans/'
tblproperties ("parquet.compression"="lzo");