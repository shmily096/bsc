-- hive sql
-- function:batch,material维度看货物销售时间leadtime
-- 按pgi日期进行分区存储，每个分区存储的是当前已pgi的所有订单处理，发货处理，运输时间信息
-- 行数据表示batch,material下对应的所有订单处理，发货处理，运输时间信息
-- history:
-- 2021-06-27    amanda   v1.0    init

drop table if exists dws_sale_order_leadtime_daily_trans;
create external table dws_sale_order_leadtime_daily_trans
(    
    delivery_id                    string comment 'dn id'
    ,material                      string comment 'sku'
    ,batch                         string comment 'batch'
    ,item_type                     string comment 'Item Type'
    ,so_create_datetime            string comment 'so创建时间' 
    ,so_dn_datetime                string comment 'so dn单创建时间' 
    ,actual_gi_date                string comment 'pgi时间' 
    ,receiving_confirmation_date   string comment '收货时间'  
    ,order_processing              float comment  '订单处理leadtime'
    ,pgi_processing                float comment  '发货处理leadtime'
    ,transport                     float comment  '运输时间leadtime'
    ,product_sale                  float comment  '货物销售leadtime'
) comment '货物销售时间leadtime'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_sale_order_leadtime_daily_trans/'
tblproperties ("parquet.compression"="lzo");

