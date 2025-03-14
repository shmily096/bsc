-- Hive SQL
-- Function： get the sales order,domestic sto, workorder by the QR code and pickup plant
---Line data: a QR code related so,sto,wo information
-- History: 
-- 2021-06-15    Donny   v1.0    init

drop table if exists dws_so_sto_wo_daily_trans;
create external table dws_so_sto_wo_daily_trans
(
    material                    string comment 'SKU' 
    ,batch                      string comment 'Batch Number' 
    ,so_dn_no                   string comment 'Sales Order DN' 
    ,so_no                      string comment 'Sales ID'  
    ,qr_code                    string comment 'QR Code' 
    ,domestic_sto_dn            string comment 'Domestic sto dn' 
    ,domestic_sto               string comment 'Domestic sto' 
    ,work_order_no              string comment 'Work Order No' 
    ,import_dn                  string comment 'Import DN' 
    ,dn_detail_dt               string
) comment 'The relation of SO,STO and WO'
partitioned by(dt string, plant string) 
stored as parquet
location '/bsc/opsdw/dws/dws_so_sto_wo_daily_trans/'
tblproperties ("parquet.compression"="lzo");