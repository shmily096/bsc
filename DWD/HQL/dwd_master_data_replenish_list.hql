-- Hive SQL
-- Function： 
-- History: 
-- 2021-05-07    Donny   v1.0    draft
-- 2021-05-24    Donny   v1.1    update table schema
-- 2021-05-29    Donny   v1.2    fix typo issue


------------------------------------------------------------------------------------
----------------------------type 区分 monthly aop fcycle----------------------------------------------------------------

drop table if exists dwd_master_data_replenish_list;
create external table dwd_master_data_replenish_list
(
       upn                            string 
    ,qty                            decimal(18,2) 
    ,hs_code                        string
    ,delivery_plant                 string
    ,origin_country                 string
    ,sap_upl_level4_name            string
    ,sap_upl_level5_name            string
    ,division_display_name          string
    ,distribution_properties        string
    ,transfer_price                 decimal(18,2)
    ,ctm_unit_price                 decimal(18,2)
    ,mm_standard_cost               decimal(18,2) 
    ,ctm_provisional_tax_rate       decimal(18,2)
    ,mfn_tax_rate                   decimal(18,2)   
    ,unit_price_currency            string
    ,type                           string
) 
partitioned by( dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_master_data_replenish_list/'
tblproperties ("parquet.compression"="lzo");
