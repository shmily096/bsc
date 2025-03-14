-- Hive SQL
-- Function： 库存交易记录 （DWD 层）
-- History: 
-- 2021-05-28    Donny   v1.0    init

drop table if exists dwd_fact_inventory_movement_trans;
create external table dwd_fact_inventory_movement_trans
(
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
) comment 'Inventory movement transactions'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_inventory_movement_trans/'
tblproperties ("parquet.compression"="lzo");