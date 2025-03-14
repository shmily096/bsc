-- Hive SQL
-- Function： customer business type（ODS 层）
-- History: 
-- 2021-05-25    Donny   v1.0    init

drop table if exists ods_cust_business_type;
create external table ods_cust_business_type
(
    cust_account            string comment 'Customer Account', 
    country                 string comment 'Country', 
    cust_group              string comment 'Customer Group',
    business_type           string comment 'Type of Business', 
    cust_ci                 string comment 'CI'
) comment 'Customer Business Type'
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_cust_business_type/';