-- Hive SQL
-- Function： cfda（DWD 层）
-- History: 
-- 2021-10-21   Donny   v1.0    draft

drop table if exists dwd_dim_cfda;
create external table dwd_dim_cfda
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
     , is_actived             string
     , created_date           string
     , last_modified_date     string
  ) comment 'CFDA'
partitioned by(dt string)
location '/bsc/opsdw/dwd/dwd_dim_cfda/';