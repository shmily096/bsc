-- Hive SQL
-- Function dws dsr billed
-- History: 
-- 2021-06-17    Donny   v1.0    init
-- 2021-06-28    Donny   v1.1    add new fileds
-- 2021-07-15    Winter  v1.2    add dt_year,dt_month

drop table if exists dws_dsr_dned_daily;
create external table dws_dsr_dned_daily
(
    so_no                    string 
    ,material                string 
    ,qty                     string
    ,net_dned                decimal(18,2)
    ,dn_rebate               decimal(18,2)
    ,division_display_name   string
    ,plant                   string
    ,dn_create_datetime      string
    ,if_cr                   string
    ,upn_del_flag            string
    ,cust_del_flag           string
    ,OrderReason_del_flag    string
    ,BillType_del_flag       string
  ) comment 'dsr dned daily'
partitioned by(dt_year string, dt_month string) 
stored as parquet
location '/bsc/opsdw/dws/dws_dsr_dned_daily/'
tblproperties ("parquet.compression"="lzo")

