-- Hive SQL
-- Function： CFDA_UPN对应 数据 （ODS 层）
-- History: 
-- 2021-10-21    Donny   v1.0    draft

drop table if exists ods_cfda_upn;
create external table ods_cfda_upn
(
       registration_no        string
     , upn                    string
     , valid_fromdate         string
     , valid_enddate          string
  ) comment 'CFDA_UPN'
partitioned by(dt string)
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_cfda_upn/';