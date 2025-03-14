
-- Hive SQL
-- Function： 
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists ads_lifecycle_leadtime_division_slcyh_topic;
create external table ads_lifecycle_leadtime_division_slcyh_topic
(
    pgi_year            string
    ,pgi_month          string
    ,division           string
    ,e2e                float
    ,in_store           float
    ,so_proce           float
    ,putaway_location   string
) comment 'dwt_lifecycle_leadtime_division_slcyh_topic'
partitioned by(dt_year string,dt_month string)
row format delimited fields terminated by '\t' 
stored as textfile
location '/bsc/opsdw/ads/ads_lifecycle_leadtime_division_slcyh_topic/';