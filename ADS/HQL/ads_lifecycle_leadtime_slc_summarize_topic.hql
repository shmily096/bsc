-- Hive SQL
-- Function： 
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists ads_lifecycle_leadtime_slcyh_summarize_topic;
create external table ads_lifecycle_leadtime_slcyh_summarize_topic
(
        e2e_min             float
   ,e2e_weiavg          float -- 加权平均 
   ,e2e_median          float --中位数 
   ,e2e_max             float
   ,in_store_min        float
   ,in_store_weiavg     float -- 加权平均 
   ,in_store_median     float --中位数 
   ,in_store_max        float
   ,putaway_min         float
   ,putaway_weiavg      float -- 加权平均 
   ,putaway_median      float --中位数 
   ,putaway_max         float
   ,pgi_month           string
   ,pgi_year            string 
   ,putaway_location    string
) comment 'dwt_lifecycle_leadtime_slcyh_summarize_topic'
partitioned by(dt_year string,dt_month string)
row format delimited fields terminated by '\t' 
stored as textfile
location '/bsc/opsdw/ads/ads_lifecycle_leadtime_slcyh_summarize_topic/';




