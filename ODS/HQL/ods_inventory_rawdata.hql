-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-17    Amanda   v1.0    draft

drop table if exists ods_inventory_rawdata;
create external table ods_inventory_rawdata
(
    BU                            string
    ,UPN                          string
    ,storage_location             string
    ,Jan_inv_qty                  decimal(9,2)
    ,Feb_inv_qty                  decimal(9,2)
    ,Mar_inv_qty                  decimal(9,2)
    ,Apr_inv_qty                  decimal(9,2)
    ,May_inv_qty                  decimal(9,2)
    ,Jun_inv_qty                  decimal(9,2)
    ,Jul_inv_qty                  decimal(9,2)
    ,Aug_inv_qty                  decimal(9,2)
    ,Sep_inv_qty                  decimal(9,2)
    ,Oct_inv_qty                  decimal(9,2)
    ,Nov_inv_qty                  decimal(9,2)
    ,Dec_inv_qty                  decimal(9,2)
    ,Quantity_flag                decimal(9,2)
    ,penang_or_not                string
) comment 'inventory_rawdata'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_inventory_rawdata/'
;