-- Hive SQL
-- Function： ODS 发货单对应表
-- History: 
--  2021-05-08    Donny   v1.0    draft

drop table if exists ods_inbound_outbound_dn_mapping;
create external table ods_inbound_outbound_dn_mapping
(
    id               bigint comment 'ID',
    update_dt        string comment 'UpdateDT',
    active           string comment 'Active',
    inbound_dn       string comment 'inbound发货单编码',
    outbond_dn       string comment 'outbound发货点编码'
) comment '发货单对应表'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_inbound_outbound_dn_mapping/';