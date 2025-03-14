-- Hive SQL
-- Function： Material rebate rate（ODS 层）
-- History: 
-- 2021-06-01    Donny   v1.0    init
-- 2021-06-01    Donny   v1.1    update the fields

drop table if exists ods_material_rebate_rate;
create external table ods_material_rebate_rate
(
    id                      string comment 'BU ID',
    bu                      string comment 'master BU', 
    is_all_upn              boolean comment 'All SKUs have same rabate rate, true or false',
    is_all_cust             boolean comment 'All cust type have same rabate rate, true or false',
    cust_business_type      string comment 'Customer type of business', 
    upn_source_field        string comment 'SKU Group:PL1, PL2, PL3, Level1, Level2',
    rebate_rate             float  comment 'Rebate Rate'
) comment 'Material rebate rate'
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_material_rebate_rate/';