-- opsdw.dwd_dim_fta_duty definition
DROP TABLE IF EXISTS dwd_dim_fta_duty;
CREATE EXTERNAL TABLE `dwd_dim_fta_duty` (
  `upn` STRING,
  `coo_type` STRING,
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
  `Dec` DECIMAL(9,2),
  `dt` STRING)
USING parquet
PARTITIONED BY (dt)
LOCATION '/bsc/opsdw/dwd/dwd_dim_fta_duty';


--- Init data
SELECT  
      upn
       ,coo_type
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
FROM opsdw.ods_fta_duty_list
