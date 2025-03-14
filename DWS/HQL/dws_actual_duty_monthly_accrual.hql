

-- Hive SQL
-- Function： dws_actual_duty_monthly_accrual
-- History: 
-- 2021-11-25    Amanda   v1.0    init

drop table if exists dws_actual_duty_lastweek_accrual;
create external table dws_actual_duty_lastweek_accrual
(
             upn                         string
            ,hs_code                     string
            ,qty1                        decimal(18,2)
            ,qty2                        decimal(18,2)
            ,paid_qty                    decimal(18,2)
            ,distribution_properties     string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,exchange_rate               decimal(18,2)
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
            ,saving_duty_rate            decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,paid_amount                 decimal(18,2)
            ,today_accrual_duty          decimal(18,2)
            ,total_accrual_duty          decimal(18,2)
            ,vat                         decimal(18,2)
            ,today_accrual_duty_usd      decimal(18,2)
            ,total_accrual_duty_usd      decimal(18,2)
            ,vat_usd                     decimal(18,2)
) comment 'dws_actual_duty_lastweek_accrual'
partitioned by(dt_year string, dt_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_actual_duty_lastweek_accrual/'
tblproperties ("parquet.compression"="lzo")





drop table if exists dws_actual_duty_lastday_accrual;
create external table dws_actual_duty_lastday_accrual
(
             upn                         string
            ,hs_code                     string
            ,qty1                        decimal(18,2)
            ,qty2                        decimal(18,2)
            ,paid_qty                    decimal(18,2)
            ,distribution_properties     string
            ,origin_country              string
            ,sap_upl_level4_name         string
            ,sap_upl_level5_name         string
            ,division_name               string
            ,reason_code                 string
            ,exchange_rate               decimal(18,2)
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
            ,saving_duty_rate            decimal(18,2)
            ,add_on_rate                 decimal(18,2)
            ,duty_change_rate            decimal(18,2)
            ,float_rate                  decimal(18,2)
            ,paid_amount                 decimal(18,2)
            ,today_accrual_duty          decimal(18,2)
            ,total_accrual_duty          decimal(18,2)
            ,vat                         decimal(18,2)
            ,today_accrual_duty_usd      decimal(18,2)
            ,total_accrual_duty_usd      decimal(18,2)
            ,vat_usd                     decimal(18,2)
) comment 'dws_actual_duty_lastday_accrual'
partitioned by(dt_year string, dt_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dws/dws_actual_duty_lastday_accrual/'
tblproperties ("parquet.compression"="lzo")