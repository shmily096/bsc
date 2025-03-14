
-- Hive SQL
-- Function： dws_dutycost_inoutbound
-- History: 
-- 2021-11-24    Amanda   v1.0    init

drop table if exists dwt_actual_duty_monthly_release;
create external table dwt_actual_duty_monthly_release
(
       division_name             string
      ,total_accrual_duty        decimal(18,2)
      ,total_accrual_duty_usd    decimal(18,2)
      ,actual_paid               decimal(18,2)
      ,actual_paid_usd           decimal(18,2)
      ,actual_duty_release       decimal(18,2)
      ,actual_duty_release_usd   decimal(18,2)
      ,actual_payment            decimal(18,2)
      ,actual_payment_usd        decimal(18,2)
) comment 'dwt_actual_duty_monthly_release'
partitioned by(dt_year string, dt_month string, dt string) 
stored as parquet
location '/bsc/opsdw/dwt/dwt_actual_duty_monthly_release/'
tblproperties ("parquet.compression"="lzo")
