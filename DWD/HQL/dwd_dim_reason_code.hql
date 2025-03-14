-- opsdw.ods_buffer_hold_list definition

drop table if exists `opsdw`.`dwd_dim_reason_code`
create external table dwd_dim_reason_code
(
    `reason_code` STRING,
    `reason_name` STRING,
    `comment` STRING,
    `ValidFrom`  DATE,
    `ValidTo`  DATE
) 
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_reason_code/'
tblproperties ("parquet.compression"="lzo");