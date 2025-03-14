-- opsdw.dwd_fact_tp_validation_report definition

DROP TABLE IF EXISTS dwd_fact_tp_validation_report;
CREATE EXTERNAL TABLE `dwd_fact_tp_validation_report` (
  `material` STRING,
  `customer_location` STRING,
  `vendor_location` STRING,
  `prodh_level4` STRING,
  `prodh_level5` STRING,
  `material_indicator` STRING,
  `vaild_from_date` STRING,
  `vaild_to_date` STRING,
  `currency_code` STRING,
  `pricing_condiction_level` STRING,
  `prodh_description_level4` STRING,
  `prodh_description_level5` STRING,
  `transfer_price` DECIMAL(9,2),
  `dt` STRING)
USING parquet
PARTITIONED BY (dt)
LOCATION '/bsc/opsdw/dwd/dwd_fact_tp_validation_report';

-- Init Data
SELECT  
        material
       ,customer_location
       ,vendor_location
       ,prodh_level4
       ,prodh_level5
       ,material_indicator
       ,vaild_from_date
       ,vaild_to_date
       ,currency_code
       ,pricing_condiction_level
       ,prodh_description_level4
       ,prodh_description_level5
       ,transfer_price
       ,dt
FROM opsdw.ods_tp_validation_report
