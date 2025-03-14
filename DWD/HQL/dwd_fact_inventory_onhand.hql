-- Hive SQL
-- Function： 日现有量 （DWD 层）
-- History: 
-- 2021-05-26    Donny   v1.0    init

drop table if exists dwd_fact_inventory_onhand;
create external table dwd_fact_inventory_onhand
(
    trans_date        string,
    inventory_type    string,
    plant             string,
    storage_loc       string,
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
    update_date       string
) comment 'Daily Inventory Onhand'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_inventory_onhand/'
tblproperties ("parquet.compression"="lzo");