-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-17    Amanda   v1.0    draft

drop table if exists ods_inbound_rawdata;
create external table ods_inbound_rawdata
(
    UPN                               string
    ,storage_location                 string
    ,Jan_inbound_qty                  decimal(9,2)
    ,Feb_inbound_qty                  decimal(9,2)
    ,Mar_inbound_qty                  decimal(9,2)
    ,Apr_inbound_qty                  decimal(9,2)
    ,May_inbound_qty                  decimal(9,2)
    ,Jun_inbound_qty                  decimal(9,2)
    ,Jul_inbound_qty                  decimal(9,2)
    ,Aug_inbound_qty                  decimal(9,2)
    ,Sep_inbound_qty                  decimal(9,2)
    ,Oct_inbound_qty                  decimal(9,2)
    ,Nov_inbound_qty                  decimal(9,2)
    ,Dec_inbound_qty                  decimal(9,2)
    ,Quantity_flag                    decimal(9,2)
) comment 'inbound_rawdata'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_inbound_rawdata/'
;