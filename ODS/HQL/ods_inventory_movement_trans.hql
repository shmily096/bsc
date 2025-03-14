-- Hive SQL
-- Function： 库存交易记录
-- History: 
--  2021-05-21    Donny   v1.0    init

drop table if exists ods_inventory_movement_trans;
create external table ods_inventory_movement_trans
(
    update_dt         string,
    movement_type     string,
    reason_code       string,
    special_stock     string,
    material_doc      string,
    mat_item          bigint,
    stock_location    string,
    plant             string,
    material          string,
    batch             string,
    qty               bigint,
    sle_dbbd          string,
    posting_date      string,
    mov_time          string,
    user_name         string,
    delivery_no       string,
    po_number         string,
    po_item           bigint,
    header_text       string,
    original_reference string,
    enter_date        string
) comment 'Inventory transactions'
partitioned by (dt string) -- 按时间分区
row format delimited fields terminated by '\t' 
STORED AS
    INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/bsc/opsdw/ods/ods_inventory_transactions/';