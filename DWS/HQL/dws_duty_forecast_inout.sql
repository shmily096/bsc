-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-24    Amanda   v1.0    init

drop table if exists dws_duty_forecast_inout;
create external table dws_duty_forecast_inout
(           upn                          string
            ,hs_code                     string
            ,qty1                        decimal(18,2)
            ,qty2                        decimal(18,2)
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,exchange_rate               decimal(18,2)
            ,fta_country                 string
            ,flag_buffer                 string
            ,flag_in_out                 string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,cal_tp                      decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,ori_rate                    decimal(18,2)
            ,coo_rate                    decimal(18,2)
            ,actual_rate                 decimal(18,2)
            ,saving_duty_rate            decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,exemption_saving            decimal(18,2)
            ,exemption_addon             decimal(18,2)
            ,exemption_change            decimal(18,2)
            ,fore_basicduty_inout        decimal(18,2)
            ,vat                         decimal(18,2)
            ,fore_duty_inout             decimal(18,2)
            ,fore_basicduty_inout_usd    decimal(18,2)
            ,vat_usd                     decimal(18,2)
            ,fore_duty_inout_usd         decimal(18,2)
            ,state                      string
  ) comment 'dws_duty_forecast_inout mr'
partitioned by(dt_year string, dt_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_inout/'
tblproperties ("parquet.compression"="lzo")






drop table if exists dws_duty_forecast_inout_aop;
create external table dws_duty_forecast_inout_aop
(           upn                          string
            ,hs_code                     string
            ,qty1                        decimal(18,2)
            ,qty2                        decimal(18,2)
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,exchange_rate               decimal(18,2)
            ,fta_country                 string
            ,flag_buffer                 string
            ,flag_in_out                 string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,cal_tp                      decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,ori_rate                    decimal(18,2)
            ,coo_rate                    decimal(18,2)
            ,actual_rate                 decimal(18,2)
            ,saving_duty_rate            decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,exemption_saving            decimal(18,2)
            ,exemption_addon             decimal(18,2)
            ,exemption_change            decimal(18,2)
            ,fore_basicduty_inout        decimal(18,2)
            ,vat                         decimal(18,2)
            ,fore_duty_inout             decimal(18,2)
            ,fore_basicduty_inout_usd    decimal(18,2)
            ,vat_usd                     decimal(18,2)
            ,fore_duty_inout_usd         decimal(18,2)
            ,state                       string
  ) comment 'dws_duty_forecast_inout_aop mr'
partitioned by(dt_year string, aop_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_inout_aop/'
tblproperties ("parquet.compression"="lzo")




drop table if exists dws_duty_forecast_inout_fcycle;
create external table dws_duty_forecast_inout_fcycle
(           upn                          string
            ,hs_code                     string
            ,qty1                        decimal(18,2)
            ,qty2                        decimal(18,2)
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,exchange_rate               decimal(18,2)
            ,fta_country                 string
            ,flag_buffer                 string
            ,flag_in_out                 string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,cal_tp                      decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,ori_rate                    decimal(18,2)
            ,coo_rate                    decimal(18,2)
            ,actual_rate                 decimal(18,2)
            ,saving_duty_rate            decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,exemption_saving            decimal(18,2)
            ,exemption_addon             decimal(18,2)
            ,exemption_change            decimal(18,2)
            ,fore_basicduty_inout        decimal(18,2)
            ,vat                         decimal(18,2)
            ,fore_duty_inout             decimal(18,2)
            ,fore_basicduty_inout_usd    decimal(18,2)
            ,vat_usd                     decimal(18,2)
            ,fore_duty_inout_usd         decimal(18,2)
            ,state                       string
  ) comment 'dws_duty_forecast_inout_fcycle mr'
partitioned by(dt_year string, fcycle_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_inout_fcycle/'
tblproperties ("parquet.compression"="lzo")