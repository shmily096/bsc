
-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-24    Amanda   v1.0    init

drop table if exists dwt_actual_duty_monthly_accrual;
create external table dwt_actual_duty_monthly_accrual
(
       division_name                  string
       ,accrual_duty                  decimal(18,2)
       ,vat                           decimal(18,2)
       ,total_accrual_duty            decimal(18,2)
       ,accrual_duty_usd              decimal(18,2)
       ,vat_usd                       decimal(18,2)
       ,total_accrual_duty_usd        decimal(18,2)
) comment 'dwt_actual_duty_monthly_accrual'
partitioned by(dt_year string, dt_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_actual_duty_monthly_accrual/'
tblproperties ("parquet.compression"="lzo")