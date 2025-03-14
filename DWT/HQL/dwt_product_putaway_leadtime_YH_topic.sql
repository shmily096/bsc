-- hive sql
-- function： 年月汇总货物上架leadtime
-- 按putaway日期年月进行分区存储,每个分区存储的是当前已putaway的所有本地化,本仓上架,min max median 加权平均
-- 行数据表示年月对应的所有本地化,本仓上架,  min max median 加权平均
-- history:
-- 2021-06-27    amanda   v1.0    init

drop table if exists dwt_product_putaway_leadtime_yh_topic;
create external table dwt_product_putaway_leadtime_yh_topic
(
    localization_min       float
    ,localization_weiavg   float 
    ,localization_median   float 
	,localization_max      float 
    ,domestic_trans_min    float
    ,domestic_trans_weiavg float
    ,domestic_trans_median float
	,domestic_trans_max    float
	,putaway_min           float
	,putaway_weiavg        float 
	,putaway_median        float 
	,putaway_max           float
	,yh_putaway_min        float
	,yh_putaway_weiavg     float 
	,yh_putaway_median     float
	,yh_putaway_max        float 
	,yh_qty                bigint
    ,putaway_month         string 
    ,putaway_year          string
	,dom_wo_no             string
) comment 'External WHS putaway leadtime'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_product_putaway_leadtime_yh_topic/'
tblproperties ("parquet.compression"="lzo");
