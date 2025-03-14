-- Hive SQL
-- Function： 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue

drop table if exists dwd_sap_except_LP05;
create external table dwd_sap_except_LP05
(
    upn string
    ,qty decimal(18,2)
) COMMENT 'dwd_sap_except_LP05'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_sap_except_LP05/'
tblproperties ("parquet.compression"="lzo");