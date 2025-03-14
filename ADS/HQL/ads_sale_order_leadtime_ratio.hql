-- Hive SQL
-- Function： 年月看货物销售leadtime的同比环比
-- History: 
-- 2021-06-17    Donny   v1.0    init

drop table if exists ads_sale_order_leadtime_ratio;
create external table ads_sale_order_leadtime_ratio
(
    dt                     string comment '统计日期'
    ,pgi_month             string comment '年'
    ,pgi_year              string comment '月'
    ,item_type             string comment 'type of business'
    ,m_product_sale_ratio  float comment   '货物销售 环比'        
    ,m_order_proce_ratio   float comment  '订单处理 环比'
    ,m_pgi_proce_ratio     float comment '发货处理环比' 
    ,m_transport_ratio     float comment '运输环比'
    ,y_product_sale_ratio  float comment   '货物销售 同比'        
    ,y_order_proce_ratio   float comment  '订单处理 同比'
    ,y_pgi_proce_ratio     float comment '发货处理同比' 
    ,y_transport_ratio     float comment '运输同比'   
  ) comment '货物销售leadtime的同比环比'
stored as parquet
location '/bsc/opsdw/ads/ads_sale_order_leadtime_ratio/'
tblproperties ("parquet.compression"="lzo");


