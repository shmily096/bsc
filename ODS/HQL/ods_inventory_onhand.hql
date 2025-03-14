-- Hive SQL
-- Function： 日现有量 （ODS 层）
-- History: 
-- 2021-05-26    Donny   v1.0    init

drop table if exists ods_inventory_onhand;
create external table ods_inventory_onhand
(
    update_dt         string,
    active            string,
    trans_date        string,
    inventory_type    string,
    plant_from        string,
    plant_to          string,
    storage_loc       string,
    pdt               bigint,
    pgi_date          string,
    delivery          string,
    delivery_line     bigint,
    marked_in_house   string,
    transport_order   string,
    profic_center     string,
    material          string,
    batch             string,
    quantity          bigint,
    unrestricted      bigint,
    inspection        bigint,
    blocked_material  bigint,
    expiration_date   string,
    standard_cost     decimal(18,2),
    extended_cost     decimal(18,2),
    qn_info           string,
    update_date       string,
    eom_ym            string,
    bu_flag           string,
    ur_qty_flag       string,
    qi_qty_flag       string,
    blk_qty_flag      string,
    expiration_date_flag string,
    short_dated_shelf_flag string,
    inbound_delivery  string
) comment 'Daily Inventory Onhand'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_inventory_onhand/';
