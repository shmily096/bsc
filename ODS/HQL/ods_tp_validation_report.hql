-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-15    Amanda   v1.0    draft

drop table if exists ods_TP_vaildation_report;
create external table ods_TP_vaildation_report
(
      material                                  string
      ,customer_location                        string
      ,vendor_location                          string
      ,ProdH_level4                             string
      ,ProdH_level5                             string
      ,material_indicator                       string
      ,vaild_from_date                          string
      ,vaild_to_date                            string
      ,currency_code                            string
      ,pricing_condiction_level                 string
      ,ProdH_description_level4                 string
      ,ProdH_description_level5                 string
      ,transfer_price                           decimal(9,2)
) comment 'TP_vaildation_report'
partitioned by(dt string)
row format delimited fields terminated by ',' 
location '/bsc/opsdw/ods/ods_TP_vaildation_report/'
;