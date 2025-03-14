CREATE TABLE public.offline_transaction (
	division string,
	net_billed string,
	material string,
	bill_date string ,
	bill_qty string,
	billed_rebate string,
	so_no string,
	customer_code string
);
-- opsdw.dwd_dim_offline_transaction definition

CREATE EXTERNAL TABLE opsdw.ods_offline_transaction(
	division string,
	net_billed string,
	material string,
	bill_date string ,
	bill_qty string,
	billed_rebate string,
	so_no string,
	customer_code string,
	updatetime timestamp)
COMMENT 'dsr_billeddaily 补充表'
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\001', 
  'serialization.format'='\001') 
STORED AS INPUTFORMAT 
  'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://hadoop-master:8020/bsc/opsdw/ods/ods_offline_transaction'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'last_modified_by'='root', 
  'last_modified_time'='1660102724', 
  'transient_lastDdlTime'='1673197473');



-- opsdw.dwd_dim_offline_transaction definition

CREATE EXTERNAL TABLE `opsdw.dwd_dim_offline_transaction`(
	division string,
	net_billed string,
	material string,
	bill_qty string,
	billed_rebate string,
	so_no string,
	customer_code string,
	updatetime timestamp)
COMMENT 'DutybyUPN'
PARTITIONED BY ( 
  `bill_date` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://hadoop-master:8020/bsc/opsdw/dwd/dwd_dim_offline_transaction'
TBLPROPERTIES (
  'bucketing_version'='2', 
  'parquet.compression'='lzo', 
  'transient_lastDdlTime'='1668414910');
