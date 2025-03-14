
-- Hive SQL
-- Function： 
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists ads_leadtime_imported_topic;
create external table ads_leadtime_imported_topic
(
    inter_trans_min             float comment 'international trans min leadtime '
    ,inter_trans_weiavg         float comment 'international trans加权平均'
    ,inter_trans_median         float comment 'international trans中位数'
    ,inter_trans_max            float comment 'international trans max leadtime'
    ,migo_min                   float comment 'min migo leadtime'
    ,migo_weiavg                float comment '加权平均migo leadtime'
    ,migo_median                float comment '中位数 migo leadtime'
    ,migo_max                   float comment 'max migo leadtime'
    ,inbound_min                float comment 'min 货物进出口 leadtime'
    ,inbound_weiavg             float comment '加权平均货物进出口leadtime'
    ,inbound_median             float comment '中位数 货物进出口 leadtime'
    ,inbound_max                float comment 'max 货物进出口leadtime'
    ,import_record_min          float comment 'min 进境备案 leadtime'
    ,import_recordweiavg        float comment '加权平均进境备案 leadtime'
    ,import_record_median       float comment '中位数 进境备案 leadtime'
    ,import_record_max          float comment 'max进境备案 leadtime'
    ,import_record_wo_num       bigint comment '进境备案数量'
    ,declar_month               string comment '月'
    ,declar_year                string comment '年'
    ) comment 'dwt_imported_topic'
partitioned by(dt_year string,dt_month string)
row format delimited fields terminated by '\t' 
stored as textfile
location '/bsc/opsdw/ads/ads_leadtime_imported_topic/';