-- Hive SQL
-- Function�� dws dsr billed
-- History: 
-- 2021-06-17    Donny   v1.0    init
-- 2021-06-28    Donny   v1.1    add new fileds
-- 2021-07-15    Winter  v1.2    add dt_year,dt_month

drop table if exists dws_dsr_fulfill_daily;
create external table dws_dsr_fulfill_daily
(
    open_qty   			string,
	total_onhand_qty    string,
	order_datetime      string,
	material            string,
	net_value           decimal(18,2),
	is_cr               string,
	division            string,
	rebate_rate         decimal(18,2),
	plant               string,
	total_open_qty      string,
	total_value         decimal(18,2)
  ) comment 'dsr fulfill daily'
partitioned by(dt_year string,dt_month string) 
stored as parquet
location '/bsc/opsdw/dws/dws_dsr_fulfill_daily/'
tblproperties ("parquet.compression"="lzo")