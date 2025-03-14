-- Hive SQL
-- Function： customer business type
-- History: 
-- 2021-05-25    Donny   v1.0    init

drop table if exists dwd_dim_cust_business_type;
create external table dwd_dim_cust_business_type
(
    cust_account            string comment 'Customer Account', 
    country                 string comment 'Country', 
    cust_group              string comment 'Customer Group',
    business_type           string comment 'Type of Business', 
    cust_ci                 string comment 'CI'
) comment 'Customer business type'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_cust_business_type/'
tblproperties ("parquet.compression"="lzo");