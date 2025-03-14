-- Hive SQL
-- Function： DSR主题宽表 （DWT 层）
-- History: 
-- 2021-06-21    Donny   v1.0    init
-- 2021-07-15    Winter  v1.1    add LE,srr
-- 2021-07-13    Amanda  v1.2    update fields

drop table if exists dwt_dsr_topic;
create external table dwt_dsr_topic
(
   bill_month                 string,
   bill_year                  string,
   division                   string,
   net_amount_shiped          decimal(18,2),
   net_amount_dn              decimal(18,2),
   fulfilled_value            decimal(18,2),
   rebate_value               decimal(18,2),
   net_cr_shiped              decimal(18,2),
   estimate_index             decimal(18,2),
   shiped_and_fulfill         decimal(18,2),
   net_amount_shiped_usd      decimal(18,2),
   net_amount_dn_usd          decimal(18,2),
   fulfilled_value_usd        decimal(18,2),
   rebate_value_usd           decimal(18,2),
   net_cr_shiped_usd          decimal(18,2),
   estimate_index_usd         decimal(18,2),
   shiped_and_fulfill_usd     decimal(18,2),
   le_cny                     decimal(18,2),
   le_usd                     decimal(18,2),
   srr_cny                    decimal(18,2),
   srr_usd                    decimal(18,2)
) comment 'dsr'
partitioned by(dt_year string,dt_month string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_dsr_topic/'
tblproperties ("parquet.compression"="lzo");
