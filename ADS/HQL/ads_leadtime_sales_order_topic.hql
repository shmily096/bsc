
-- Hive SQL
-- Function： 
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists ads_leadtime_sales_order_topic;
create external table ads_leadtime_sales_order_topic
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
        ) comment 'dwt_sales_order_leadtime_topic'
partitioned by(dt_year string,dt_month string)
row format delimited fields terminated by '\t' 
stored as textfile
location '/bsc/opsdw/ads/ads_leadtime_sales_order_topic/';