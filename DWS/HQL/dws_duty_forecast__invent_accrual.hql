
--------------------------------------monthly-------------------------------------------------------------
drop table if exists dws_duty_forecast_invent_accrual_mid;
create external table dws_duty_forecast_invent_accrual_mid
(
             upn                         string
            ,qty                         decimal(18,2)
            ,hs_code                     string
            ,delivery_plant              string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_display_name       string
            ,distribution_properties     string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,unit_price_currency         string
            ,type                        string
            ,state                       string
) comment 'dws_duty_forecast_invent_accrual_mid'
partitioned by(month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_invent_accrual_mid/'
tblproperties ("parquet.compression"="lzo")


-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists dws_duty_forecast_invent_accrual;
create external table dws_duty_forecast_invent_accrual
(
             upn                         string
            ,hs_code                     string
            ,inv_qty                     decimal(18,2)
            ,qty2                        decimal(18,2)
            ,buffer_flag                 string
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,exchange_rate               decimal(18,2)
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,fta_country                 string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,cal_tp                      decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,ori_rate                    decimal(18,2)
            ,coo_rate                    decimal(18,2)
            ,actual_rate                 decimal(18,2)
            ,cal_rate                    decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,fore_invent_accrual         decimal(18,2)
            ,fore_invent_accrual_usd     decimal(18,2)
) comment 'dws_duty_forecast_invent_accrual'
partitioned by(dt_year string, dt_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_invent_accrual/'
tblproperties ("parquet.compression"="lzo")


----------------------------------------aop------------------------------------------------------

drop table if exists dws_duty_forecast_invent_accrual_mid_aop;
create external table dws_duty_forecast_invent_accrual_mid_aop
(            upn                         string
            ,qty                         decimal(18,2)
            ,hs_code                     string
            ,delivery_plant              string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_display_name       string
            ,distribution_properties     string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,unit_price_currency         string
            ,type                        string
            ,state                       string
) comment 'dws_duty_forecast_invent_accrual_mid_aop'
partitioned by(aop_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_invent_accrual_mid_aop/'
tblproperties ("parquet.compression"="lzo")


-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists dws_duty_forecast_invent_accrual_aop;
create external table dws_duty_forecast_invent_accrual_aop
(
             upn                         string
            ,hs_code                     string
            ,inv_qty                     decimal(18,2)
            ,qty2                        decimal(18,2)
            ,buffer_qty                  decimal(18,2)
            ,buffer_flag                 string
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,exchange_rate               decimal(18,2)
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,fta_country                 string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,cal_tp                      decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,ori_rate                    decimal(18,2)
            ,coo_rate                    decimal(18,2)
            ,actual_rate                 decimal(18,2)
            ,cal_rate                    decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,fore_invent_accrual         decimal(18,2)
            ,fore_invent_accrual_usd     decimal(18,2)
) comment 'dws_duty_forecast_invent_accrual_aop'
partitioned by(dt_year string, aop_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_invent_accrual_aop/'
tblproperties ("parquet.compression"="lzo")



---------------------------------------fcycle-------------------------------------------------------
drop table if exists dws_duty_forecast_invent_accrual_mid_fcycle;
create external table dws_duty_forecast_invent_accrual_mid_fcycle
(
            upn                         string
            ,hs_code                     string
            ,inv_qty                     decimal(18,2)
            ,buffer_qty                  decimal(18,2)
            ,buffer_flag                 string
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,exchange_rate               decimal(18,2)
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,cal_rate                    decimal(18,2) 
) comment 'dws_duty_forecast_invent_accrual_mid_fcycle'
partitioned by(dt_year string, fcycle_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_invent_accrual_mid_fcycle/'
tblproperties ("parquet.compression"="lzo")



-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists dws_duty_forecast_invent_accrual_fcycle;
create external table dws_duty_forecast_invent_accrual_fcycle
(
             upn                         string
            ,hs_code                     string
            ,inv_qty                     decimal(18,2)
            ,qty2                        decimal(18,2)
            ,buffer_qty                  decimal(18,2)
            ,buffer_flag                 string
            ,delivery_plant              string
            ,distribution_properties     string
            ,origin_country              string
            ,exchange_rate               decimal(18,2)
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,fta_country                 string
            ,transfer_price              decimal(18,2)
            ,ctm_unit_price              decimal(18,2)
            ,mm_standard_cost            decimal(18,2)
            ,cal_tp                      decimal(18,2)
            ,ctm_provisional_tax_rate    decimal(18,2)
            ,mfn_tax_rate                decimal(18,2)
            ,ori_rate                    decimal(18,2)
            ,coo_rate                    decimal(18,2)
            ,actual_rate                 decimal(18,2)
            ,cal_rate                    decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,fore_invent_accrual         decimal(18,2)
            ,fore_invent_accrual_usd     decimal(18,2)
) comment 'dws_duty_forecast_invent_accrual_fcycle'
partitioned by(dt_year string, fcycle_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_duty_forecast_invent_accrual_fcycle/'
tblproperties ("parquet.compression"="lzo")