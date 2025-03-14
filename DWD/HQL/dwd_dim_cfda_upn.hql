-- Hive SQL
-- Function： cfda_upn（DWD 层）
-- History: 
-- 2021-10-21   Donny   v1.0    draft

drop table if exists dwd_dim_cfda_upn;
create external table dwd_dim_cfda_upn
(
       registration_no        string
     , upn                    string
     , valid_fromdate         string
     , valid_enddate          string
  ) comment 'CFDA_UPN'
partitioned by(dt string)
location '/bsc/opsdw/dwd/dwd_dim_cfda_upn/'
;

