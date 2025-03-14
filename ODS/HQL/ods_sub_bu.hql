-- Hive SQL
-- Function： Sub BU（ODS 层）
-- History: 
-- 2021-06-01    Donny   v1.0    init
-- 2021-06-01    Donny   v1.1    update the table name

drop table if exists ods_sub_bu;
create external table ods_sub_bu
(
    bu                      string comment 'master BU', 
    cust_business_type      string comment 'Customer type of business', 
    source_field            string comment 'Customer Group:PL1, PL2, PL3, Level1, Level2',
    sub_bu                  string comment 'Sub BU'
) comment 'Sub BU'
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_sub_bu/';