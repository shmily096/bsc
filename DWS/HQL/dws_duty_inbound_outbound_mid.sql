drop table if exists dws_duty_inbound_outbound_mid;
create external table dws_duty_inbound_outbound_mid
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
    ,state                          string 
    ,flag_in_out                    string
  ) comment 'dws_duty_inbound_outbound_mid mr'
partitioned by(month string,dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_inbound_outbound_mid/'
tblproperties ("parquet.compression"="lzo")


drop table if exists dws_duty_inbound_outbound_mid_aop;
create external table dws_duty_inbound_outbound_mid_aop
(    upn                            string 
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
    ,state                          string 
    ,flag_in_out                    string
  ) comment 'dws_duty_inbound_outbound_mid_aop mr'
partitioned by(aop_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_inbound_outbound_mid_aop/'
tblproperties ("parquet.compression"="lzo")



drop table if exists dws_duty_inbound_outbound_mid_fcycle;
create external table dws_duty_inbound_outbound_mid_fcycle
(
    upn                            string 
    ,qty                           decimal(18,2) 
    ,sap_level4                     string
    ,sap_level5                     string
    ,bu                             string
    ,standard_cost                  decimal(18,2) 
    ,delivery_plant                 string
    ,buffer_flag                    string
    ,distribution_properties       string
    ,mfn_tax_rate                  decimal(18,2) 
    ,hs_code                       string
    ,unit_price                     decimal(18,2) 
    ,provisional_tax_rate           decimal(18,2) 
    ,origin_country                 string
    ,exchange_rate                  decimal(18,2)
    ,state                          string 
    ,flag_in_out                    string
  ) comment 'dws_duty_inbound_outbound_mid_fcycle mr'
partitioned by(fcycle_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_inbound_outbound_mid_fcycle/'
tblproperties ("parquet.compression"="lzo")