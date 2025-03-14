-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-10-21    Donny   v1.0    draft

drop table if exists ods_cfda;
create external table ods_cfda
(
       registration_no        string
     , devision_id            string
     , upn_name               string
     , product_name_en        string
     , commerce_name          string
     , gm_kind                int
     , gm_catalog             string
     , gm_catelog_new         string
     , manufacturer           string
     , manufacturer_address   string
     , manufacturingsite_address string
     , register_info          string
     , register_address       string
     , sterilization_mode     string
     , sterilization_validity string
     , service_year           string
     , product_material       string
     , storage_conditions     string
     , storage_conditions_temperature string
     , transport_condition    string
     , transport_condition_temperature string
     , valid_fromdate         string
     , valid_enddate          string
     , is_actived             boolean
     , created_date           string
     , last_modified_date     string
  ) comment 'CFDA'
partitioned by(dt string)
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_cfda/'
;