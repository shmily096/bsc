-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-24    Amanda   v1.0    init

drop table if exists dwt_duty_forecast_inout;
create external table dwt_duty_forecast_inout
(
    division_name              string
    ,fore_basicduty_inout      decimal(18,2)
    ,vat                       decimal(18,2)
    ,fore_duty_inout           decimal(18,2)
    ,fore_basicduty_inout_usd  decimal(18,2)
    ,vat_usd                   decimal(18,2)
    ,fore_duty_inout_usd       decimal(18,2)
    ,exemption_saving          decimal(18,2)--US trade war exemption saving
    ,exemption_saving_usd          decimal(18,2)--Saving or increase from other assumption
  ) comment 'dwt_duty_forecast_inout'
partitioned by(dt_year string, dt_month string, dt_quarter string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_duty_forecast_inout/'
tblproperties ("parquet.compression"="lzo")



drop table if exists dwt_duty_forecast_finalduty;
create external table dwt_duty_forecast_finalduty
(
   division_name                 string
    ,fore_basicduty_inout        decimal(18,2)
    ,vat                         decimal(18,2)
    ,fore_duty_inout             decimal(18,2)
    ,invent_accrual_realease     decimal(18,2)
    ,forecast_final_duty         decimal(18,2)
    ,fore_basicduty_inout_usd    decimal(18,2)
    ,vat_usd                     decimal(18,2)
    ,fore_duty_inout_usd         decimal(18,2)
    ,invent_accrual_realease_usd decimal(18,2)
    ,forecast_final_duty_usd     decimal(18,2)
  ) comment 'dwt_duty_forecast_finalduty'
partitioned by(dt_year string, dt_month string, dt_quarter string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_duty_forecast_finalduty/'
tblproperties ("parquet.compression"="lzo")


drop table if exists dwt_duty_forecast_cr_penang_saving;
create external table dwt_duty_forecast_cr_penang_saving
(
    division_name              string
    ,fta_country               string
    ,fta_saving                decimal(18,2)
    ,fta_saving_usd            decimal(18,2)
  ) comment 'dwt_duty_forecast_cr_penang_saving'
partitioned by(dt_year string, dt_month string, dt_quarter string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_duty_forecast_cr_penang_saving/'
tblproperties ("parquet.compression"="lzo")


drop table if exists dwt_duty_forecast_addon_change;
create external table dwt_duty_forecast_addon_change
(
    division_name              string
    ,exemption_addon      decimal(18,2)
    ,exemption_change      decimal(18,2)
    ,exemption_addon_usd      decimal(18,2)
    ,exemption_change_usd      decimal(18,2)
  ) comment 'dwt_duty_forecast_addon_change'
partitioned by(dt_year string, dt_month string, dt_quarter string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_duty_forecast_addon_change/'
tblproperties ("parquet.compression"="lzo")