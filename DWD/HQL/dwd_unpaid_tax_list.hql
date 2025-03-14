
-- Hive SQL
-- Function： 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue

drop table if exists dwd_unpaid_tax_list;
create external table dwd_unpaid_tax_list
(
    state string
    ,upn string
    ,qty decimal(18,2)
) COMMENT 'dwd_unpaid_tax_list'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_unpaid_tax_list/'
tblproperties ("parquet.compression"="lzo");