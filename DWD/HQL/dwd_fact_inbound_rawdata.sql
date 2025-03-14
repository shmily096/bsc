drop table if exists dwd_fact_inbound_rawdata;
create external table dwd_fact_inbound_rawdata
(
     UPN                      string
    ,storage_location         string
    ,Jan_qty                  decimal(9,2)
    ,Feb_qty                  decimal(9,2)
    ,Mar_qty                  decimal(9,2)
    ,Apr_qty                  decimal(9,2)
    ,May_qty                  decimal(9,2)
    ,Jun_qty                  decimal(9,2)
    ,Jul_qty                  decimal(9,2)
    ,Aug_qty                  decimal(9,2)
    ,Sep_qty                  decimal(9,2)
    ,Oct_qty                  decimal(9,2)
    ,Nov_qty                  decimal(9,2)
    ,Dec_qty                  decimal(9,2)
    ,quantity_flag            decimal(9,2)
) 
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_inbound_rawdata/'
tblproperties ("parquet.compression"="lzo");

-- Data from 
SELECT  
    upn
    ,storage_location
    ,Jan_qty
    ,Feb_qty
    ,Mar_qty
    ,Apr_qty
    ,May_qty
    ,Jun_qty
    ,Jul_qty
    ,Aug_qty
    ,Sep_qty
    ,Oct_qty
    ,Nov_qty
    ,Dec_qty
    ,quantity_flag
    ,dt
FROM opsdw.ods_inbound_rawdata;

