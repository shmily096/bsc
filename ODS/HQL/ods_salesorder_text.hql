-- Hive SQL
-- Function：TRANS_SalesOrder_Text （ODS 层）
-- History: 
-- 2021-08-11    Donny   v1.0    draft

drop table if exists ods_salesorder_text ;
create external table ods_salesorder_text
(
    so_no  string comment '销售订单编码',
    pick_list_remark   string comment '备注'

) comment 'SalesOrder_Text'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_salesorder_text/';