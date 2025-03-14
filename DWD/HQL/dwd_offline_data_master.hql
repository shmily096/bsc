drop table if exists dwd_offline_data_master;
create external table dwd_offline_data_master
(
           upn                          string
           ,qty                          string 
           ,hs_code                      string
           ,delivery_plant               string
           ,origin_country               string
           ,sap_upl_level4_name          string
           ,sap_upl_level5_name          string
           ,division_display_name        string
           ,distribution_properties      string
           ,transfer_price               decimal(18,2)
           ,ctm_unit_price               decimal(18,2) 
           ,mm_standard_cost             decimal(18,2)
           ,ctm_provisional_tax_rate     decimal(18,2)
           ,mfn_tax_rate                 decimal(18,2)
           ,unit_price_currency         string
           ,type                         string
) comment 'dwd_offline_data_master'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dws/dwd_offline_data_master/'
tblproperties ("parquet.compression"="lzo")