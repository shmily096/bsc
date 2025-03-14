-- Hive SQL
-- Function： sub BU维度和cust_level3维度看经销商完成和指标
-- 按创建bill日期进行分区存储，每个分区存储的是当前已Bill的所有订单经销商信息
-- 行数据表示每天BU，CustLevel3,delar 完成指标
-- History: 
-- 2021-06-17    Donny   v1.0    init
-- 2021-06-28    Donny   v1.1    add new fileds

drop table if exists dws_dsr_dealer_daily_transation;
create external table dws_dsr_dealer_daily_transation
(
    division                      string comment 'BU'
    ,sub_division                 string comment 'Sub Division'
    ,customer_code                string comment 'Sold to code'
    ,cust_level3                  string comment 'cust level3'
    ,bill_month                   string comment 'bill month'
    ,bill_year                    string comment 'bill year' 
    ,bill_date                    string comment 'bill date ' 
    ,quarter                      string
    ,dealer_name                  string comment 'dealer name' 
    ,parent_dealer_name           string comment 'parent dealer name' 
    ,dealer_complete              decimal(16,2) comment 'dealer complete' -- 经销商完成额
    ,dealer_mon_target            decimal(16,2) comment ' dealer_mon_target  ' -- 经销商月指标
    ,dealer_mon_total_target      decimal(16,2)
    ,dt_year                      string
    ,dt_month                     string
  ) comment 'sub BU维度和cust_level3维度看经销商完成和指标'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_dsr_dealer_daily_transation/'
tblproperties ("parquet.compression"="lzo");
