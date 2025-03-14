-- Hive SQL
-- Function： 年月看进口leadtime的同比环比
-- History: 
-- 2021-07-08    Donny   v1.0    init

drop table if exists ads_imported_ratio;
create external table ads_imported_ratio
(
    dt                      string comment '统计日期'
    ,declar_month            string comment '年'
    ,declar_year            string comment '月'
    ,m_inter_trans_ratio    float comment   'international trans 环比'        
    ,m_migo_ratio           float comment  'migo 环比'
    ,m_inbound_ratio        float comment '进口环比' 
    ,m_import_record_ratio  float comment '进境备案环比'
    ,m_import_qty_ratio     float comment  '进境报关数量 环比'
    ,y_inter_trans_ratio    float comment 'international trans 同比'  
    ,y_migo_ratio           float comment  'migo 同比'
    ,y_inbound_ratio        float comment    '进口同比' 
    ,y_import_record_ratio  float comment   '进境备案同比'
    ,y_import_qty_ratio     float comment   '进境报关数量 同比'
  ) comment '进口leadtime的同比环比'
stored as parquet
location '/bsc/opsdw/ads/ads_imported_ratio/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 年月看进口leadtime的同比环比
-- History:
-- 2021-07-08    Amanda   v1.0    init

drop table if exists ads_lifecycle_leadtime_slcyh_ratio;
create external table ads_lifecycle_leadtime_slcyh_ratio
(
    dt                      string comment '统计日期'
    ,putaway_location       string
    ,pgi_year               string comment '月'
    ,pgi_month              string comment '年'
    ,m_e2e_ratio            float
    ,m_in_store_ratio       float
    ,m_putaway_ratio        float
    ,y_e2e_ratio            float
    ,y_in_store_ratio       float
    ,y_putaway_ratio        float
) comment 'e2e slc yhleadtime的同比环比'
--partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/ads/ads_lifecycle_leadtime_slcyh_ratio/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 年月看SLC本地化的同比环比
-- History: 
-- 2021-07-07    Donny   v1.0    init

drop table if exists ads_product_putaway_leadtime_slc_ratio;
create external table ads_product_putaway_leadtime_slc_ratio
(
  dt                         string comment '统计日期'
  ,putaway_month              string comment '年'
  ,putaway_year               string comment '月'
  ,m_localization_ratio       float comment 'localization 环比'
  ,m_putaway_ratio            float comment 'putaway 环比'
  ,m_slc_putaway_ratio        float comment 'slc_putaway环比'
  ,m_slc_qty_ratio            float comment '本地化数量环比'
  ,m_local_wo_no_ratio        float comment '本地化工单 环比'
  ,y_localization_ratio       float comment 'localization 同比'
  ,y_putaway_ratio            float comment 'putaway 同比'
  ,y_slc_putaway_ratio        float comment 'slc_putaway 同比'
  ,y_slc_qty_ratio            float comment '本地化数量 同比'
  ,y_local_wo_no_ratio        float comment '本地化工单 同比'
) comment 'SLC本地化的同比环比'
stored as parquet
location '/bsc/opsdw/ads/ads_product_putaway_leadtime_slc_ratio/'
tblproperties ("parquet.compression"="lzo");

-- Hive SQL
-- Function： 年月看YH本地化的同比环比
-- History: 
-- 2021-07-08    Donny   v1.0    init

drop table if exists ads_product_putaway_leadtime_yh_ratio;
create external table ads_product_putaway_leadtime_yh_ratio
(
  dt                        string comment '统计日期'
  ,putaway_month            string comment '年'
  ,putaway_year             string comment '月'
  ,m_localization_ratio     float comment   'localization 环比'
  ,m_domestic_trans_ratio   float comment  'domestic_trans_ratio 环比'
  ,m_putaway_ratio          float comment  'putaway 环比'
  ,m_yh_putaway_ratio       float comment 'yh_putaway环比' 
  ,m_yh_qty_ratio           float comment '外仓转移数量环比'
  ,m_yh_dom_wo_no_ratio     float comment'domestic dn# 环比'
  ,y_localization_ratio     float comment   'localization 同比'  
  ,y_domestic_trans_ratio   float comment  'domestic_trans_ratio 同比'
  ,y_putaway_ratio          float comment  'putaway 同比'
  ,y_yh_putaway_ratio       float comment 'yh_putaway同比' 
  ,y_yh_qty_ratio           float comment '外仓转移数量同比'
  ,y_yh_dom_wo_no_ratio     float comment 'domestic dn#同比'
) comment 'YH本地化的同比环比'
stored as parquet
location '/bsc/opsdw/ads/ads_product_putaway_leadtime_yh_ratio/'
tblproperties ("parquet.compression"="lzo");

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