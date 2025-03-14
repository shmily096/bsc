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
