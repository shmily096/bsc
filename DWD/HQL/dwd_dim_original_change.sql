-- dwd_dim_original_change

-- opsdw.ods_orginial_country_change definition

CREATE  external TABLE `opsdw`.`dwd_dim_original_change` (
  `upn` STRING,
  `coo` STRING,
  `qty` DECIMAL(9,2),
  `vaild_from` DATE,
  `vaild_to` DATE,
  `dt` STRING)
USING parquet
PARTITIONED BY (dt)
LOCATION '/bsc/opsdw/dwd/dwd_dim_original_change';


SELECT  upn
       ,coo
       ,qty
       ,vaild_from
       ,vaild_to
       ,dt
FROM opsdw.ods_orginial_country_change;
 