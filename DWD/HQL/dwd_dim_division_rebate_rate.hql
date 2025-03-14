-- Hive SQL
-- Function： Division Rebate Rate （DWD 层）
-- History: 
-- 2021-06-08    Donny   v1.0    inti

drop table if exists dwd_dim_division_rebate_rate;
create external table dwd_dim_division_rebate_rate
(
    id                      string comment 'division ID',
    division                string comment 'master division', 
    cust_business_type      array<string> comment 'Customer type of business rule 1', 
    sub_divison             string comment 'UPN PL1, PL2, PL3, Level1, Level2 rule 2',
    rate                    float comment 'Rate rate',
    default_rate            float comment 'Sub BU'
) comment 'Division Rebate Rate'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_division_rebate_rate/'
tblproperties ("parquet.compression"="lzo");