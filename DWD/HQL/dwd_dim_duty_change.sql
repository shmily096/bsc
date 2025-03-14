-- opsdw.ods_duty_change_list definition
drop table if exists dwd_dim_duty_change;
CREATE EXTERNAL TABLE `opsdw`.`dwd_dim_duty_change` (
  `hscode` STRING,
  `coo` STRING,
  `reason_code` STRING,
  `duty_change_rate` DECIMAL(9,2),
  `vaild_from` DATE,
  `vaild_to` DATE,
  `dt` STRING)
USING parquet
PARTITIONED BY (dt)
LOCATION '/bsc/opsdw/dwd/dwd_dim_duty_change';


