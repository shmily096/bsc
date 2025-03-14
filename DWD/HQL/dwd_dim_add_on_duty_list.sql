drop table if exists dwd_dim_add_on_duty_list;
create external table dwd_dim_add_on_duty_list
(
    `hs_code` STRING,
    `country_code` STRING,
    `Jan` DECIMAL(9,2),
    `Feb` DECIMAL(9,2),
    `Mar` DECIMAL(9,2),
    `Apr` DECIMAL(9,2),
    `May` DECIMAL(9,2),
    `Jun` DECIMAL(9,2),
    `Jul` DECIMAL(9,2),
    `Aug` DECIMAL(9,2),
    `Sep` DECIMAL(9,2),
    `Oct` DECIMAL(9,2),
    `Nov` DECIMAL(9,2),
    `Dec` DECIMAL(9,2)

) comment '申请加征关税清单'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_add_on_duty_list/'
tblproperties ("parquet.compression"="lzo");