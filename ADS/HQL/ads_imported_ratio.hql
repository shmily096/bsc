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
--partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/ads/ads_imported_ratio/'
tblproperties ("parquet.compression"="lzo");

