-- Hive SQL
-- Function： 进出口状态事实表 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-14    Donny   v1.1    update fields information
-- 2021-05-24    Donny   v1.2    update fields information

drop table if exists dwd_fact_import_export_declaration_info;
create external table dwd_fact_import_export_declaration_info
(
    commercial_invoice string,
    update_date       string,
    bsc_inform_slc_date string comment '通知SLC时间',
    t1_pick_up_date   string,
    actual_arrival_time string comment '航班到港时间',
    dock_warrant_date string comment '仓单确认时间',
    invoice_receiving_date string comment '邮件通知预报时间',
    forwording        string,
    into_inventory_date string comment '实际到仓时间',
    shipment_internal_number string,
    master_bill_no    string,
    house_waybill_no  string,
    import_export_flag string,
    shipment_type     string,
    quantity          string,
    gross_weight      string,
    department        string,
    country_area      string,
    transportation_type string,
    etd               string comment '预计航班起飞时间',
    eta               string comment '预计航班到达时间',
    revise_etd        string,
    revise_eta        string,
    commodity_inspection string,
    customs_inspection string,
    declaration_start_date string comment '报关开始时间', -- ods forwording_inform_slc_pick
    declaration_completion_date string comment '报关结束时间',
    related_delivery_no string
) comment '进出口状态'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_import_export_declaration_info/'
tblproperties ("parquet.compression"="lzo");