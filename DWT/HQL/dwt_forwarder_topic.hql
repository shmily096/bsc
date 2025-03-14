-- Hive SQL
-- Function：
-- History:
-- 2021-06-24    Donny   v1.0    init

drop table if exists dwt_forwarder_topic;
create external table dwt_forwarder_topic
(
    forwarder           string,
    pick_up_median      float,
    `month`                     string,
    `year`                      string
  ) comment '货运代理主題'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_forwarder_topic/'
tblproperties ("parquet.compression"="lzo");