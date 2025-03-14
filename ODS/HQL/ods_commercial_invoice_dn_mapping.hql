-- Hive SQL
-- Function： ODS 商业发票与发货单对应表
-- History: 
-- 2021-05-08    Donny   v1.0    draft
-- 2021-05-20    Donny   v1.1    update field information

drop table if exists ods_commercial_invoice_dn_mapping;
create external table ods_commercial_invoice_dn_mapping
(
    id                int comment 'ID',
    update_dt         string comment 'UpdateDT',
    active            string comment 'Active',
    delivery          string comment '发货单',
    invoice           string comment '商业发票',
    qty               bigint,
    mail_received     string comment '预报时间'
    sap_migo_date     string comment 'MIGO'
) comment '商业发票与发货单对应表'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_commercial_invoice_dn_mapping/';