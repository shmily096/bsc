-- Hive SQL
-- Function： Workorder and QR code mapping（ODS 层）
-- History: 
-- 2021-06-23    Donny   v1.0    init

drop table if exists ods_work_order_qr_code_mapping;
create external table ods_work_order_qr_code_mapping
(
    plant_id            string comment 'Plant Id', 
    work_order_no       string comment 'Work Order Number', 
    dn_no               string comment 'Related DN number',
    material            string comment 'Material',
    batch               string comment 'Batch Number', 
    qr_code             string comment 'QR Code'
) comment 'Work order and QR code Mapping'
partitioned by(dt string)
row format delimited fields terminated by ',' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_work_order_qr_code_mapping/';