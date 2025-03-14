-- Hive SQL
-- Function：KNVI （ODS 层）
-- History: 
-- 2021-08-11    Donny   v1.0    draft

drop table if exists ods_customermaster_knvi ;
create external table ods_customermaster_knvi
(
    cust_account   string comment 'CustomerNumber',
    tax_category     string comment 'TaxCategory',
    tax_classification string comment 'TaxClassification'

) comment 'KNVI'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_customermaster_knvi/';