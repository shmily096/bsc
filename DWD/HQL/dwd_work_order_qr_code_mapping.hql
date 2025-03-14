-- Hive SQL
-- Function： Workorder and QR code mapping（DWD 层）
-- History: 
-- 2021-06-23    Donny   v1.0    init

drop table if exists dwd_fact_work_order_qr_code_mapping;
create external table dwd_fact_work_order_qr_code_mapping
(
    plant_id            string comment 'Plant Id', 
    work_order_no       string comment 'Work Order Number', 
    dn_no               string comment 'Related DN number',
    material            string comment 'Material',
    batch               string comment 'Batch Number', 
    qr_code             string comment 'QR Code'
) comment 'Work order and QR code Mapping'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_work_order_qr_code_mapping/'
tblproperties ("parquet.compression"="lzo");