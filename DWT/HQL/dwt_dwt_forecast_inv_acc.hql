
-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-24    Amanda   v1.0    init

drop table if exists dwt_forecast_invent_accrual;
create external table dwt_forecast_invent_accrual
(
       division_name                    string
      ,fore_invent_accrual              decimal(18,2)
      ,fore_invent_accrual_usd          decimal(18,2)
) comment 'dwt_forecast_invent_accrual'
partitioned by(dt_year string, dt_month string, dt_quarter string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_forecast_invent_accrual/'
tblproperties ("parquet.compression"="lzo")


drop table if exists dwt_forecast_inv_acc_release;
create external table dwt_forecast_inv_acc_release
(
       division_name                    string
      ,fore_invent_accrual              decimal(18,2)
      ,fore_invent_accrual_usd          decimal(18,2)
      ,invent_accrual_realease          decimal(18,2)
      ,invent_accrual_realease_usd      decimal(18,2)
) comment 'dwt_forecast_inv_acc_release'
partitioned by(dt_year string, dt_month string, dt_quarter string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_forecast_inv_acc_release/'
tblproperties ("parquet.compression"="lzo")