-- Hive SQL
-- Function： dws dsr CR daily
-- History: 
-- 2021-07-22    Amanda   v1.0    init

drop table if exists dws_dsr_cr_daily;
create external table dws_dsr_cr_daily
(
    so_no                   string 
    ,bill_date               string 
    ,material                string 
    ,net_cr                  decimal(18,2) 
    ,division_display_name   string
    ,upn_del_flag            string
    ,cust_del_flag           string
    ,OrderReason_del_flag    string
    ,BillType_del_flag       string
    ,dt_year                 string
    ,dt_month                string
  ) comment 'dsr cr daily'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_dsr_cr_daily/'
tblproperties ("parquet.compression"="lzo")
