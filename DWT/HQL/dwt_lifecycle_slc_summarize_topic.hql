-- Hive SQL
-- Function： lifecycle leadtime slc min max weiavg median
-- History: 
-- 2021-07-08    Amanda   v1.0   init

drop table if exists dwt_lifecycle_leadtime_slc_summarize_topic;
create external table dwt_lifecycle_leadtime_slc_summarize_topic
(
    e2e_min             float
    ,e2e_weiavg         float -- 加权平均 
    ,e2e_median         float --中位数 
    ,e2e_max            float
    ,in_store_min       float
    ,in_store_weiavg    float -- 加权平均 
    ,in_store_median    float --中位数 
    ,in_store_max       float
    ,putaway_min        float
    ,putaway_weiavg     float -- 加权平均 
    ,putaway_median     float --中位数 
    ,putaway_max        float
    ,pgi_month          string
    ,pgi_year           string 
) comment 'lifecycle leadtime slc'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_lifecycle_leadtime_slc_summarize_topic/'
tblproperties ("parquet.compression"="lzo");
