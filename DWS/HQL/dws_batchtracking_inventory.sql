-- Hive SQL
-- Function： dws dsr CR daily
-- History: 
-- 2021-07-22    Amanda   v1.0    init

drop table if exists dws_batchtracking_inventoryonhand;
create external table dws_batchtracking_inventoryonhand
(
    transaction_date                   string 
    ,inventroy_type               string 
    ,site                string 
    ,storage_location       string  
    ,material
    ,batch
    ,profit_center
    ,total_qty
    ,unrestricted_qty
    ,inspection_qty
    ,blocked_qty
    ,country_of_origin
    ,date_of_manufacture2
    ,extra_info
  ) comment 'dws_batchtracking_inventoryonhand'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_batchtracking_inventoryonhand/'
tblproperties ("parquet.compression"="lzo")