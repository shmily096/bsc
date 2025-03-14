-- Hive SQL
-- Function： ODS 进出口inbound_tracking
-- History: 
-- 2021-05-08    Donny   v1.0    draft

drop table if exists ods_shipment_status_inbound_tracking;
create external table ods_shipment_status_inbound_tracking
(
    id                bigint,
    update_dt         string,
    active            string,
    work_number       string,
    commercial_invoice string,
    bsc_inform_slc_date string,
    t1_pick_up_date   string,
    actual_arrival_time string,
    dock_warrant_date string,
    forwording_inform_slc_pick string,
    forwording        string,
    into_inventory_date string,
    update_date       string,
    shipment_internal_number string,
    master_bill_no    string,
    house_waybill_no  string,
    import_export_flag string,
    shipment_type     string,
    emergency_signs   string,
    merchandiser      string,
    voucher_maker     string,
    abnormal_causes1  string,
    abnormal_causes2  string,
    inspection_mark1  string,
    inspection_mark2  string,
    inspection_mark3  string,
    remark            string,
    quantity          string,
    gross_weight      string,
    forwarder_service_level string,
    department        string,
    country_area      string,
    transportation_type string,
    customs_supervision_certificate string,
    commodity_inspection_demand string,
    customized_certificate string,
    etd               string,
    eta               string,
    revise_etd        string,
    revise_eta        string,
    commodity_inspection string,
    customs_inspection string,
    declaration_completion_date string
) comment '进出口inbound_tracking'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_shipment_status_inbound_tracking/';