-- Hive SQL
-- Function�� dws dsr billed
-- History: 
-- 2021-06-17    Donny   v1.0    init
-- 2021-06-28    Donny   v1.1    add new fileds
-- 2021-07-15    Winter  v1.2    add dt_year,dt_month

drop table if exists dws_dsr_billed_daily;
create external table dws_dsr_billed_daily
(
            so_no           string
           ,net_billed      decimal(18,2)
           ,bill_date       string
           ,material        string
           ,billed_rebate   decimal(18,2)
           ,division        string
           ,sub_division    string
           ,upn_del_flag    string
           ,cust_del_flag   string
           ,OrderReason_del_flag   string
           ,BillType_del_flag      string
           ,customer_code   string
           ,dt_year         string
           ,dt_month        string
  ) comment 'dsr billed daily'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_dsr_billed_daily/'
tblproperties ("parquet.compression"="lzo");
