-- Hive SQL
-- Function： 
-- History: 
-- 2021-06-24    Donny   v1.0    init

drop table if exists dwt_plant_topic;
create external table dwt_plant_topic
(
    plant_id                    string,
    inter_trans_median          float,
    `month`                     string,
    `year`                      string
  ) comment 'T1Plant主題'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_plant_topic/'
tblproperties ("parquet.compression"="lzo");