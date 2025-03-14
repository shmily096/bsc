-- Hive SQL
-- Function： Rebate Rate（ODS 层）
-- History: 
-- 2021-06-03    Donny   v1.0    init
-- 2021-06-07    Donny   v1.1    update the name
-- 2021-06-25    Donny   v1.2    update the name

drop table if exists ods_division_rebate_rate;
create external table ods_division_rebate_rate
(
    id                      string comment 'division ID',
    division                string comment 'master division', 
    cust_business_type      array<string> comment 'Customer type of business rule 1', 
    sub_divison             string comment 'UPN PL1, PL2, PL3, Level1, Level2 rule 2',
    rate                    float comment 'Rate rate',
    default_rate            float comment 'Sub BU'
) comment 'Division Rebate Rate'
row format delimited fields terminated by '\t'
collection items terminated by ',' 
map keys terminated by ':'
location '/bsc/opsdw/ods/ods_division_rebate_rate/';