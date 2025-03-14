drop table if exists dwt_dsr_dealer_topic;
create external table dwt_dsr_dealer_topic
(
    division                      string comment 'BU'
    ,sub_division                 string comment 'Sub Division'
    ,customer_code                string comment 'Sold to code'
    ,cust_level3                  string comment 'cust level3'
    ,bill_month                   string comment 'bill month'
    ,bill_year                    string comment 'bill year' 
    ,quarter                       string
    ,dealer_name                  string comment 'dealer name' 
    ,parent_dealer_name           string
    ,dealer_mon_complete          decimal(16,2)
    ,dealer_mon_target            decimal(16,2) comment 'dealer target per month' -- 经销商月完成额
  ) comment '经销商完成和指标'
partitioned by(dt_year string, dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_dsr_dealer_topic/'
tblproperties ("parquet.compression"="lzo");