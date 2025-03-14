-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-17    Amanda   v1.0    draft

drop table if exists ods_outbound_rawdata;
create external table ods_outbound_rawdata
(
    UPN                               string
    ,storage_location                 string
    ,Jan_outbound_qty                  decimal(9,2)
    ,Feb_outbound_qty                  decimal(9,2)
    ,Mar_outbound_qty                  decimal(9,2)
    ,Apr_outbound_qty                  decimal(9,2)
    ,May_outbound_qty                  decimal(9,2)
    ,Jun_outbound_qty                  decimal(9,2)
    ,Jul_outbound_qty                  decimal(9,2)
    ,Aug_outbound_qty                  decimal(9,2)
    ,Sep_outbound_qty                  decimal(9,2)
    ,Oct_outbound_qty                  decimal(9,2)
    ,Nov_outbound_qty                  decimal(9,2)
    ,Dec_outbound_qty                  decimal(9,2)
    ,Quantity_flag                    decimal(9,2)
) comment 'outbound_rawdata'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_outbound_rawdata/'
;