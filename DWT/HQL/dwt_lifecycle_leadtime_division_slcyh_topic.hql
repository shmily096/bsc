-- Hive SQL
-- Function： division年月看slc yh e2e leadtime
-- History:
-- 2021-07-09    Donny   v1.0    init

drop table if exists dwt_lifecycle_leadtime_division_slcyh_topic;
create external table dwt_lifecycle_leadtime_division_slcyh_topic
(
    pgi_year            string
    ,pgi_month          string
    ,division           string
    ,e2e                float
    ,in_store           float
    ,so_proce           float
    ,putaway_location   string
) comment ' division年月看slc yh e2e leadtime'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_lifecycle_leadtime_division_slcyh_topic/'
tblproperties ("parquet.compression"="lzo");
