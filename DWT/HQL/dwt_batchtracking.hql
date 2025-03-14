-- Hive SQL
-- Function： 
-- History: 
-- 2021-09-14    Donny   v1.0    init

drop table if exists dwt_batchtracking_salesdn;
create external table dwt_batchtracking_salesdn
(
      so_no               string
     , material           string
     , batch              string
     , delivery_id        string
     , delry_qty          int
     , dn_createdtime     string
     , dn_pgitime         string
     , dn_receivedtime    string
     , order_type         string
     , order_reason       string
     , ItemType           string
     , SOStuatus          string
     , customer_code      string
     , DealerType         string
     , sub_divison        string
     , pick_up_plant      string
     , SO_createdtime     string
     , SearchPeriod       string
     , extra_info         string
  ) comment 'BatchTracking_SalesDnInfo'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_batchtracking_salesdn/'
tblproperties ("parquet.compression"="lzo");

drop table if exists dwt_batchtracking_inventoryonhand;
create external table dwt_batchtracking_inventoryonhand
(
       transaction_date      string
     , inventory_type        string
     , site                  string
     , storage_location      string
     , material              string
     , batch                 string
     , profit_center         string
     , total_qty             int
     , unrestricted_qty      int
     , inspection_qty        int
     , blocked_qty           int
     , extra_info            string
  ) comment 'BatchTracking_InventoryOnhand'
stored as parquet
location '/bsc/opsdw/dwt/dwt_batchtracking_inventoryonhand/'
tblproperties ("parquet.compression"="lzo");


drop table if exists dwt_batchtracking_importdn;
create external table dwt_batchtracking_importdn
(
       sto_no                 string
     , delivery_no            string
     , material               string
     , batch                  string
     , dn_qty                 int
     , sto_qty                int
     , sto_orderstatus        string 
     , ship_from_plant        string
     , ship_to_plant          string
     , sto_createtime         string
     , sto_pgitime            string
     , sto_migotime           string
     , extra_info             string
  ) comment 'BatchTracking_importdn'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_batchtracking_importdn/'
tblproperties ("parquet.compression"="lzo");