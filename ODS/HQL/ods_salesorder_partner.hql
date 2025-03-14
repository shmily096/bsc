-- Hive SQL
-- Function：SalesOrder_Partner （ODS 层）
-- History: 
-- 2021-08-11    Donny   v1.0    draft

drop table if exists ods_salesorder_partner ;
create external table ods_salesorder_partner
(
    so_no  string comment '销售订单编码',
    customer_function   string comment '功能',
    customer_shipto  string comment 'ShipTO',
    carrier   string comment '承运商'

) comment 'SalesOrder_Partner'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_salesorder_partner/';