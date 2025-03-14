-- opsdw.ods_buffer_list definition

drop table if exists `opsdw`.`dwd_dim_buffer_list`;
create external table `opsdw`.`dwd_dim_buffer_list` (
  `bu` STRING,
  `upn` STRING,
  `vaild_from` STRING,
  `vaild_to` STRING,
  `flag` STRING,
  dt_year string,
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
LOCATION '/bsc/opsdw/dwd/dwd_dim_buffer_list';


SELECT  bu
       ,upn
       ,vaild_from
       ,vaild_to
       ,flag
       ,'2021'
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
       ,`Dec`
       ,dt
FROM opsdw.ods_buffer_list

union all

SELECT  bu
       ,upn
       ,vaild_from
       ,vaild_to
       ,flag
       ,'2022'
       ,Jan_next
       ,Feb_next
       ,Mar_next
       ,Apr_next
       ,May_next
       ,Jun_next
       ,Jul_next
       ,Aug_next
       ,Sep_next
       ,Oct_next
       ,Nov_next
       ,Dec_next
       ,dt
FROM opsdw.ods_buffer_list
