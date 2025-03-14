with a as (
SELECT 
    division_name
    ,exchange_rate 
    ,sum(fore_basicduty_inout) as fore_basicduty_inout
    ,sum(vat) as vat
    ,sum(fore_duty_inout) as fore_duty_inout
    ,sum(fore_basicduty_inout_usd) as fore_basicduty_inout_usd
    ,sum(vat_usd) as vat_usd
    ,sum(fore_duty_inout_usd) as fore_duty_inout_usd
    ,fta_country
    ,sum(cal_tp * (-coo_rate)) AS fta_saving --CR Saving&Penang Saving
    ,sum(exemption_addon) as exemption_addon--US trade war add on Duty
    ,sum(exemption_saving) as exemption_saving--US trade war exemption saving
    ,sum(exemption_change) as exemption_change--Saving or increase from other assumption (例如政策调整的导致的多缴或者少缴税金）
    ,dt_year
    ,dt_month
    ,case when dt_month < 4 then 1
            when dt_month < 7 then 2
            when dt_month < 10 then 3
            else 4
       end as dt_quarter
FROM dws_duty_forecast_inout
WHERE dt IN (SELECT max(dt) FROM dws_duty_forecast_inout)
group by division_name, fta_country, dt_year, dt_month,exchange_rate
)


--insert overwrite table dwt_duty_forecast_inout partition(dt_year, dt_month, dt_quarter, dt)
--select 
--    division_name
--    ,sum(fore_basicduty_inout) as fore_basicduty_inout
--    ,sum(vat) as vat
--    ,sum(fore_duty_inout) as fore_duty_inout
--    ,sum(fore_basicduty_inout_usd) as fore_basicduty_inout_usd
--    ,sum(vat_usd) as vat_usd
--    ,sum(fore_duty_inout_usd) as fore_duty_inout_usd
--    ,sum(exemption_saving) as exemption_saving--US trade war exemption saving
--    ,sum(exemption_saving / exchange_rate) as exemption_saving_usd
--    ,dt_year as dt_year
--    ,dt_month as dt_month
--    ,dt_quarter as dt_quarter
--    ,'2021-12-28' as dt
--from a
--group by
--     division_name   
--    ,dt_year 
--    ,dt_month 
--    ,dt_quarter 



------------------------------------------------------------------------------------

 
   -- ,exemption_addon--US trade war add on Duty
    --,exemption_change--Saving or increase from other assumption (例如政策调整的导致的多缴或者少缴税金）
,a_ac as(
SELECT 
    division_name
    ,sum(exemption_addon) as exemption_addon--US trade war add on Duty
    ,sum(exemption_change) as exemption_change--Saving or increase from other assumption (例如政策调整的导致的多缴或者少缴税金）
    ,sum(exemption_addon/exchange_rate ) as exemption_addon_usd
    ,sum(exemption_change/exchange_rate ) as exemption_change_usd
    ,dt_year
    ,dt_month
    ,dt_quarter
FROM a
group by
     division_name
    ,exchange_rate 
    ,dt_year
    ,dt_month
    ,dt_quarter
)
,b_ac as(
SELECT 
    division_name,
    sum(add_on_rate*cal_tp) as exemption_addon,
    sum(duty_change_rate*cal_tp) as exemption_change,
    sum(add_on_rate*cal_tp/exchange_rate ) as exemption_addon_usd,
    sum(duty_change_rate*cal_tp/exchange_rate ) as exemption_change_usd,
       dt_year,
       dt_month,
       case when dt_month < 4 then 1
            when dt_month < 7 then 2
            when dt_month < 10 then 3
            else 4
       end as dt_quarter
from dws_duty_forecast__invent_accrual
where dt_year='2021' and dt_month='12'
      and fta_country is not null
      and dt in (select max(dt) from dws_duty_forecast__invent_accrual)
group BY 
      division_name,
       exchange_rate,
       dt_year,
       dt_month
)
,ab_ac as(
select division_name
    ,exemption_addon--US trade war add on Duty
    ,exemption_change--Saving or increase from other assumption (例如政策调整的导致的多缴或者少缴税金）
    ,exemption_addon_usd
    ,exemption_change_usd
    ,dt_year
    ,dt_month
    ,dt_quarter
from a_ac
union all
select division_name
    ,exemption_addon--US trade war add on Duty
    ,exemption_change--Saving or increase from other assumption (例如政策调整的导致的多缴或者少缴税金）
    ,exemption_addon_usd
    ,exemption_change_usd
    ,dt_year
    ,dt_month
    ,dt_quarter
from b_ac
)
insert overwrite table dwt_duty_forecast_addon_change partition(dt_year, dt_month, dt_quarter, dt)
select
    division_name
    ,sum(exemption_addon) as exemption_addon--US trade war add on Duty
    ,sum(exemption_change) as exemption_change--Saving or increase from other assumption (例如政策调整的导致的多缴或者少缴税金）
    ,sum(exemption_addon_usd) as exemption_addon_usd
    ,sum(exemption_change_usd) as exemption_change_usd
    ,dt_year
    ,dt_month
    ,dt_quarter
    ,'2021-12-29' as dt
from ab_ac
group by division_name ,dt_year
    ,dt_month
    ,dt_quarter



























,a_fta as (
select division_name,
       fta_country,
       fta_saving,
       fta_saving/exchange_rate as fta_saving_usd,
       dt_year,
       dt_month,
       dt_quarter
FROM a 
where dt_year='2021' and dt_month='12'
      and fta_country is not null
group BY 
      division_name,
       exchange_rate,
       fta_country,
       fta_saving,
       dt_year,
       dt_month,
       dt_quarter
)
,b_fta as(
select division_name,
       fta_country,
       sum((-coo_rate) * cal_tp) as fta_saving,      
       sum((-coo_rate * cal_tp)/exchange_rate) as fta_saving_usd,
       dt_year,
       dt_month,
       case when dt_month < 4 then 1
            when dt_month < 7 then 2
            when dt_month < 10 then 3
            else 4
       end as dt_quarter
from dws_duty_forecast__invent_accrual
where dt_year='2021' and dt_month='12'
      and fta_country is not null
      and dt in (select max(dt) from dws_duty_forecast__invent_accrual)
group BY 
      division_name,
       exchange_rate,
       fta_country,
       dt_year,
       dt_month,
       dt_quarter
)
,ab_fta as (
select division_name,
       fta_country,
       fta_saving,
       fta_saving_usd,
       dt_year,
       dt_month,
       dt_quarter
from a_fta
union all
select division_name,
       fta_country,
       fta_saving,
       fta_saving_usd,
       dt_year,
       dt_month,
       dt_quarter
from b_fta
)

insert overwrite table dwt_duty_forecast_cr_penang_saving partition(dt_year, dt_month, dt_quarter, dt)
select  division_name,
       fta_country,
       sum(fta_saving) as fta_saving,
       sum(fta_saving_usd) as fta_saving_usd,
       dt_year,
       dt_month,
       dt_quarter,
       '2021-12-29' as dt
from ab_fta
group by
        division_name,
       fta_country,
       dt_year,
       dt_month,
       dt_quarter
    

------------------------------------------------------------------------------------
    
,aa as (
select
    division_name
    ,fore_basicduty_inout
    ,vat
    ,fore_duty_inout
    ,fore_basicduty_inout_usd
    ,vat_usd
    ,fore_duty_inout_usd
    ,dt_year
    ,dt_month
    ,dt_quarter
from a
where fore_basicduty_inout >0
group by
division_name
    ,fore_basicduty_inout
    ,vat
    ,fore_duty_inout
    ,fore_basicduty_inout_usd
    ,vat_usd
    ,fore_duty_inout_usd
    ,dt_year
    ,dt_month
    ,dt_quarter
)

insert overwrite table dwt_duty_forecast_finalduty partition(dt_year, dt_month, dt_quarter,dt)
select
    iar.division_name
    ,aa.fore_basicduty_inout
    ,aa.vat
    ,aa.fore_duty_inout
    ,iar.invent_accrual_realease
    ,if(aa.fore_duty_inout is null,iar.invent_accrual_realease ,(aa.fore_duty_inout +iar.invent_accrual_realease)) as forecast_final_duty
    ,aa.fore_basicduty_inout_usd
    ,aa.vat_usd
    ,aa.fore_duty_inout_usd
    ,iar.invent_accrual_realease_usd
    ,if(aa.fore_duty_inout_usd is null, iar.invent_accrual_realease_usd, (aa.fore_duty_inout_usd + iar.invent_accrual_realease_usd)) as forecast_final_duty_usd
    ,iar.dt_year
    ,iar.dt_month
    ,iar.dt_quarter
    ,'2021-12-29' as dt
from dwt_forecast_inv_acc_release iar
left join aa
on aa.dt_month = iar.dt_month and aa.division_name=iar.division_name
where iar.dt_quarter='4'