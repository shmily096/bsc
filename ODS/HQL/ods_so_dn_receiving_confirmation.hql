-- Hive SQL
-- Function： SO 客户签收信息 （ODS 层）
-- History: 
-- 2021-06-08    Donny   v1.0    init

drop table if exists ods_so_dn_receiving_confirmation;
create external table ods_so_dn_receiving_confirmation
(
    update_date             string comment 'APP_OPS updated date',
    delivery_no             string comment 'DN number',
    first_confirmation_date string comment 'confirmation date of first batch',
    last_confirmation_date  string comment 'confirmation date of last batch ' --used this date for SO dn
) comment 'SO DN Receiving Confirmation Date'
partitioned by (dt string) 
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_so_dn_receiving_confirmation/';