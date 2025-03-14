-- Hive SQL
-- Function： 客户主数据的层次结构 （DWD 层）
-- History: 
-- 2021-05-24    Donny   v1.0    init

drop table if exists dwd_dim_customer_level;
create external table dwd_dim_customer_level
(
    level1_code             string comment 'Level1 Code',
    level1_english_name     string comment 'Level1 English Name',
    level1_chinese_name     string comment 'Level1 Chinese Name',
    level2_code             string comment 'Level2 Code',
    level2_english_name     string comment 'Level2 English Name',
    level2_chinese_name     string comment 'Level2 Chinese Name',
    level3_code             string comment 'Level3 Code',
    level3_english_name     string comment 'Level3 English Name',
    level3_chinese_name     string comment 'Level3 Chinese Name',
    level4_code             string comment 'Level4 Code',
    level4_chinese_name     string comment 'Level4 Chinese Name',
    business_category       string comment 'Business Category'
) comment 'Customer level master data'
stored as parquet
location '/bsc/opsdw/dwd/dwd_dim_customer_level/'
tblproperties ("parquet.compression"="lzo");