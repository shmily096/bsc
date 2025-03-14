
-- opsdw.ods_outbound_rawdata definition
DROP TABLE IF EXISTS dwd_fact_outbound_rawdata;
CREATE EXTERNAL TABLE `dwd_fact_outbound_rawdata` (
  `upn` STRING,
  `storage_location` STRING,
  `Jan_qty` DECIMAL(9,2),
  `Feb_qty` DECIMAL(9,2),
  `Mar_qty` DECIMAL(9,2),
  `Apr_qty` DECIMAL(9,2),
  `May_qty` DECIMAL(9,2),
  `Jun_qty` DECIMAL(9,2),
  `Jul_qty` DECIMAL(9,2),
  `Aug_qty` DECIMAL(9,2),
  `Sep_qty` DECIMAL(9,2),
  `Oct_qty` DECIMAL(9,2),
  `Nov_qty` DECIMAL(9,2),
  `Dec_qty` DECIMAL(9,2),
  `quantity_flag` DECIMAL(9,2),
  `dt` STRING)
USING parquet
PARTITIONED BY (dt)
LOCATION '/bsc/opsdw/dwd/dwd_fact_outbound_rawdata';

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
FROM opsdw.ods_outbound_rawdata;
