-- Hive SQL
-- Function： 年月看YH本地化的同比环比
-- History: 
-- 2021-06-17    Donny   v1.0    init

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