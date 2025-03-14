
-- Hive SQL
-- Function： 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue

drop table if exists dwd_duty_by_upn;
create external table dwd_duty_by_upn
(
     `year_month` STRING,
  `bu` STRING,
  `upn` STRING,
  `qty` DECIMAL(18,2),
  `customs_declaration_no` STRING,
  `seria_number` STRING,
  `tax_number` STRING,
  `tax_type` STRING,
  `contract_no` STRING,
  `tax_bill_createdt` DATE,
  `record_area` STRING,
  `amount` STRING,
  `tax_payment_date` DATE,
  `mail_delivery_date` DATE
) COMMENT 'dwd_duty_by_upn'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_duty_by_upn/'
tblproperties ("parquet.compression"="lzo");