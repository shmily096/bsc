-- Hive SQL
-- Function： 
-- History: 
-- 2021-09-14    Donny   v1.0    init

drop table if exists dwt_dqmonitor_material;
create external table dwt_dqmonitor_material
(
     mid                          string
     ,material_code               string
     ,material_type               string
     ,delivery_plant              string
     ,default_location            string
     ,division_id                 string
     ,profit_center               string
     ,standard_cost               string
     ,sap_upl_level4_code         string
     ,sap_upl_level4_name         string
     ,sap_upl_level5_code         string
     ,sap_upl_level5_name         string
     ,source_type                 string
     ,chinese_name                string
     ,english_name                string
     ,legal_entity                string
     ,division_name               string
       ) comment 'dqmonitor'
partitioned by(dt string)
stored as parquet
location '/bsc/opsdw/dwt/dwt_dqmonitor_material/'
tblproperties ("parquet.compression"="lzo");