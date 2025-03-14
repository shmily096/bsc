-- Hive SQL
-- Function：SalesOrder_CreatedInfo （ODS 层）
-- History: 
-- 2021-08-11    Donny   v1.0    draft
drop table if exists ods_salesorder_createdinfo ;
create external table ods_salesorder_createdinfo
(
    so_no  string comment '销售订单编码',
    request_delivery_date   string comment '客户要求到货时间',
    so_create_dt    string comment '订单创建日期（时间）',
    so_create_by   string comment '订单创建人'

) comment 'SalesOrder_CreatedInfo'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_salesorder_createdinfo/';