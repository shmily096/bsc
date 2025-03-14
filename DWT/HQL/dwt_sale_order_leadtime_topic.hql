-- hive sql
-- function: 年月汇总货物销售时间leadtime
-- 按pgi日期年月进行分区存储,每个分区存储的是当前已pgi的所有订单处理,发货处理,运输时间信息
-- 行数据表示年月下对应的所有订单处理,发货处理,运输时间信息 min max median 加权平均
-- history: 
-- 2021-06-27    amanda   v1.0    init

drop table if exists dwt_sale_order_leadtime_topic;
create external table dwt_sale_order_leadtime_topic
(   
    item_type                  string comment 'Item Type' 
    ,product_sale_min          float comment 'p min '
    ,product_sale_weiavg       float comment   '加权平均'
    ,product_sale_max          float comment   'sku max'
    ,product_sale_median       float comment   '中位数'
    ,order_proce_min           float 
    ,order_proce_weiavg        float  
    ,order_proce_max           float 
    ,order_proce_median        float 
    ,pgi_proce_min             float 
    ,pgi_proce_weiavg          float 
    ,pgi_proce_max             float 
    ,pgi_proce_median          float 
    ,transport_min             float 
    ,transport_weiavg          float 
    ,transport_max             float 
    ,transport_median          float 
    ,pgi_month                 string 
    ,pgi_year                  string 
) comment '年月汇总货物销售时间leadtime'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_sale_order_leadtime_topic/'
tblproperties ("parquet.compression"="lzo");