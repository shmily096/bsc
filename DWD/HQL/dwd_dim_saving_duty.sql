-- DDL
drop table if exists dwd_dim_saving_duty;
create external table dwd_dim_saving_duty
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
) 
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_saving_duty/'
tblproperties ("parquet.compression"="lzo");


-- Data from ODS table
SELECT  hs_code
       ,country_code
       ,Jan
       ,Feb
       ,Mar
       ,Apr
       ,May
       ,Jun
       ,Jul
       ,Aug
       ,Sep
       ,Oct
       ,Nov
       ,Dec
       ,dt
FROM opsdw.ods_saving_duty_list;