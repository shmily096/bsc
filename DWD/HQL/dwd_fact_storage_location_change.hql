drop table if exists dwd_fact_storage_location_change;
create external table dwd_fact_storage_location_change
(
    material           string,
    move_flag          string,
    comment            string
) comment 'dwd_fact_storage_location_change'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_storage_location_change/'
tblproperties ("parquet.compression"="lzo");