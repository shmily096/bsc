-- Hive SQL
-- Function：KNB1 （ODS 层）
-- History: 
-- 2021-08-11    Donny   v1.0    draft

drop table if exists ods_customermaster_knb1 ;
create external table ods_customermaster_knb1
(
    cust_account   string comment 'CustomerNumber',
    account_nr     string comment 'AccountNumber',
    company_code string comment 'CompanyCode'

) comment 'KNB1'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_customermaster_knb1/';