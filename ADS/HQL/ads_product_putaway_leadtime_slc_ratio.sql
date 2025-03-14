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