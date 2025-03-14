#!/bin/bash
# Function:
#   sync up xxxx 
# History:
# 2021-11-24    Amanda   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

# if [ -n "$1" ] ;then 
#     sync_date=$1
# else
#     sync_date=$(date  +%F)
# fi

if [ -n "$1" ] ;then 
    sync_year=$1
else
    sync_year=$(date  +'%Y')
fi

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date  +%F)

echo "start syncing data into dws layer on ${sync_date[year]} :${sync_date[month]} .................."

sql_str="
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
set hive.exec.parallel=false;


with c as(
select    
    upn,
    qty,
    sap_level4 as sap_upl_level4_name,
    sap_level5 as sap_upl_level5_name,
    bu as disvision_name,
    standard_cost,
    delivery_plant,
    buffer_flag,
    distribution_properties,
    mfn_tax_rate,
    hs_code,
    unit_price,
    provisional_tax_rate,
    origin_country,
    exchange_rate as ex_rate,
    flag_in_out,
    dt
from dws_duty_inbound_outbound_mid
where dt in ( select max(dt) from dws_duty_inbound_outbound_mid)	
) 

, d_saving as (
select 
    c.upn,
    c.qty,
    c.sap_upl_level4_name,
    c.sap_upl_level5_name,
    c.disvision_name,
    c.standard_cost,
    c.delivery_plant,
    c.buffer_flag,
    c.distribution_properties,
    c.mfn_tax_rate,
    c.hs_code,
    c.unit_price,
    c.provisional_tax_rate,
    c.origin_country,
    c.ex_rate,
    c.flag_in_out,
    sdl.saving_duty_rate
from c left join 
(select 
    hs_code,
    country_code,
    case when month('2021-12-22') ='1' then Jan
         when month('2021-12-22') ='2' then Feb
         when month('2021-12-22') ='3' then Mar
         when month('2021-12-22') ='4' then Apr
         when month('2021-12-22') ='5' then May
         when month('2021-12-22') ='6' then Jun
         when month('2021-12-22') ='7' then Jul
         when month('2021-12-22') ='8' then Aug
         when month('2021-12-22') ='9' then Sep
         when month('2021-12-22') ='10' then Oct
         when month('2021-12-22') ='11' then Nov
         when month('2021-12-22') ='12' then Dec
    else 0    
    end as saving_duty_rate
from dwd_dim_saving_duty 
where dt in (select max(dt) from dwd_dim_saving_duty)) sdl
on c.hs_code = sdl.hs_code and c.origin_country = sdl.country_code
)
, d as (
select 
    d_saving.upn,
    d_saving.qty,
    d_saving.sap_upl_level4_name,
    d_saving.sap_upl_level5_name,
    d_saving.disvision_name,
    d_saving.standard_cost,
    d_saving.delivery_plant,
    d_saving.buffer_flag,
    d_saving.distribution_properties,
    d_saving.mfn_tax_rate,
    d_saving.hs_code,
    d_saving.unit_price,
    d_saving.provisional_tax_rate,
    d_saving.origin_country,
    d_saving.ex_rate,
    d_saving.flag_in_out,
    d_saving.saving_duty_rate
    ,adl.add_on_rate
from d_saving 
left join 
(select 
    hs_code, 
    country_code,
    case when month('2021-12-22') ='1' then Jan
         when month('2021-12-22') ='2' then Feb
         when month('2021-12-22') ='3' then Mar
         when month('2021-12-22') ='4' then Apr
         when month('2021-12-22') ='5' then May
         when month('2021-12-22') ='6' then Jun
         when month('2021-12-22') ='7' then Jul
         when month('2021-12-22') ='8' then Aug
         when month('2021-12-22') ='9' then Sep
         when month('2021-12-22') ='10' then Oct
         when month('2021-12-22') ='11' then Nov
         when month('2021-12-22') ='12' then Dec
    else 0    
    end as add_on_rate
from dwd_dim_add_on_duty_list 
where dt in (select max(dt) from dwd_dim_add_on_duty_list)) adl
on d_saving.hs_code = adl.hs_code and d_saving.origin_country = adl.country_code
)
,ddd as(
select 
    d.upn,
    d.qty,
    d.sap_upl_level4_name,
    d.sap_upl_level5_name,
    d.disvision_name,
    d.standard_cost,
    d.delivery_plant,
    d.buffer_flag,
    d.distribution_properties,
    d.mfn_tax_rate,
    d.hs_code,
    d.unit_price,
    d.provisional_tax_rate,
    if(coalesce(d.provisional_tax_rate, d.mfn_tax_rate) is null, 0, coalesce(d.provisional_tax_rate, d.mfn_tax_rate)) as act_rate,
    d.origin_country,
    d.ex_rate,
    d.flag_in_out,
    d.saving_duty_rate,
    d.add_on_rate,
    tvr.transfer_price
from d 
left join 
(select 
    transfer_price,
    material
from dwd_fact_tp_validation_report 
where dt in (select max(dt) from dwd_fact_tp_validation_report )
group by 
    transfer_price,
    material) tvr 
on d.upn = tvr.material
group by 
    d.upn,
    d.qty,
    d.sap_upl_level4_name,
    d.sap_upl_level5_name,
    d.disvision_name,
    d.standard_cost,
    d.delivery_plant,
    d.buffer_flag,
    d.distribution_properties,
    d.mfn_tax_rate,
    d.hs_code,
    d.unit_price,
    d.provisional_tax_rate,
    d.origin_country,
    d.ex_rate,
    d.flag_in_out,
    d.saving_duty_rate,
    d.add_on_rate,
    tvr.transfer_price
 )
,coo as(
select 
    UPN,
    coo_type as fta_country,
    case when month('2021-12-22') ='1' then Jan
         when month('2021-12-22') ='2' then Feb
         when month('2021-12-22') ='3' then Mar
         when month('2021-12-22') ='4' then Apr
         when month('2021-12-22') ='5' then May
         when month('2021-12-22') ='6' then Jun
         when month('2021-12-22') ='7' then Jul
         when month('2021-12-22') ='8' then Aug
         when month('2021-12-22') ='9' then Sep
         when month('2021-12-22') ='10' then Oct
         when month('2021-12-22') ='11' then Nov
         when month('2021-12-22') ='12' then Dec
    else 0    
    end as coo_rate
from dwd_dim_fta_duty 
where dt in (select max(dt) from dwd_dim_fta_duty)
)
,e as(
select
    ddd.upn,
    ddd.qty,
    ddd.sap_upl_level4_name,
    ddd.sap_upl_level5_name,
    ddd.disvision_name,
    ddd.standard_cost,
    ddd.delivery_plant,
    ddd.buffer_flag,
    ddd.distribution_properties,
    ddd.mfn_tax_rate,
    ddd.hs_code,
    ddd.unit_price,
    ddd.provisional_tax_rate,
    ddd.act_rate,
    ddd.origin_country,
    ddd.ex_rate,
    ddd.flag_in_out,
    ddd.saving_duty_rate,
    ddd.add_on_rate,
    ddd.transfer_price,
    coo.fta_country,
    coo.coo_rate,
    (coo.coo_rate + ddd.act_rate)  as actual_rate
from ddd 
left join coo
on ddd.upn = coo.upn
)
,ocl as
(select 
    upn, coo, qty
from dwd_dim_original_change
where dt ='2021-12-22' and vaild_to >= '2021-12-22' and vaild_from >= '2021-12-22'
)
,dcl AS 
(select
    upn, coo, reason_code, duty_change_rate
from dwd_dim_duty_change
where dt ='2021-12-22' and vaild_to >= '2021-12-22' and vaild_from >= '2021-12-22'
)
,dde as (
select
    e.upn 
    ,if(e.qty is null, 0, e.qty) as qty1
    ,if(ocl.qty is null, 0, ocl.qty) as qty2
    ,e.sap_upl_level4_name
    ,e.sap_upl_level5_name
    ,e.disvision_name
    ,e.standard_cost
    ,e.delivery_plant
    ,e.buffer_flag
    ,e.distribution_properties
    ,e.mfn_tax_rate
    ,e.hs_code
    ,e.unit_price
    ,e.provisional_tax_rate
    ,e.act_rate as ori_rate
    ,e.origin_country
    ,e.ex_rate
    ,e.flag_in_out
    ,if(e.saving_duty_rate is null, 0, e.saving_duty_rate) as saving_duty_rate
    ,if(e.add_on_rate is null, 0, e.add_on_rate) as add_on_rate
    ,e.transfer_price
    ,e.fta_country
    ,e.coo_rate
    ,if(e.actual_rate is null, 0, e.actual_rate) as actual_rate
    ,coalesce(e.transfer_price, e.unit_price, e.standard_cost*1.46) as cal_tp
    ,dcl.reason_code
    ,if(dcl.duty_change_rate is null, 0, dcl.duty_change_rate) as duty_change_rate
from e
left join  ocl
on e.upn = ocl.upn and e.origin_country = ocl.coo
left join  dcl
on e.upn = dcl.upn and e.origin_country = dcl.coo
)

,dd as (
select
    upn 
    ,qty1
    ,qty2
    ,sap_upl_level4_name
    ,sap_upl_level5_name
    ,disvision_name
    ,standard_cost
    ,delivery_plant
    ,buffer_flag
    ,distribution_properties
    ,mfn_tax_rate
    ,hs_code
    ,unit_price
    ,provisional_tax_rate
    ,ori_rate
    ,origin_country
    ,ex_rate
    ,flag_in_out
    ,saving_duty_rate
    ,add_on_rate
    ,transfer_price
    ,fta_country
    ,coo_rate
    ,actual_rate
    ,cal_tp
    ,reason_code
    ,duty_change_rate
    ,(add_on_rate - saving_duty_rate + duty_change_rate) as float_rate
from dde
)





-- vat = 0.13* forecast duty inout
-- forecast duty= qty * (coalesce(provisional_rate, mfn)+加征-减免)*coalesce(TP,ctm:unit_price,mm:standard_cost*1.46)
insert overwrite table dws_duty_forecast_inout partition(dt_year, dt_month, dt)
      select dd.upn
            ,dd.hs_code
            ,dd.qty1
            ,dd.qty2
            ,dd.delivery_plant 
            ,dd.distribution_properties
            ,dd.origin_country
            ,dd.sap_upl_level4_name
            ,dd.sap_upl_level5_name
            ,dd.disvision_name
            ,dd.reason_code
            ,dd.ex_rate
            ,dd.fta_country
            ,dd.buffer_flag
            ,dd.flag_in_out
            ,dd.transfer_price
            ,dd.unit_price
            ,dd.standard_cost
            ,dd.cal_tp
            ,dd.provisional_tax_rate
            ,dd.mfn_tax_rate
            ,dd.ori_rate
            ,dd.coo_rate
            ,cast(dd.actual_rate as decimal(18,2))
            ,dd.saving_duty_rate
            ,dd.add_on_rate
            ,dd.duty_change_rate
            ,dd.float_rate
            ,dd.qty1 * dd.saving_duty_rate * cal_tp * 0.01 as exemption_saving
            ,dd.qty1 * dd.add_on_rate * cal_tp * 0.01 as exemption_addon
            ,dd.qty1 * dd.duty_change_rate * cal_tp * 0.01 as exemption_change
            ,dd.qty1 * (dd.actual_rate + dd.float_rate)* cal_tp * 0.01 
                    + dd.qty2 * (dd.ori_rate + dd.float_rate)* cal_tp* 0.01 as fore_basicduty_inout
            ,0.13 * ((dd.qty1-dd.qty2) * (dd.actual_rate + dd.float_rate)* cal_tp * 0.01
                    + dd.qty2 * (dd.ori_rate + dd.float_rate)* cal_tp * 0.01) as vat
            ,1.13 * ((dd.qty1-dd.qty2) * (dd.actual_rate + dd.float_rate)* cal_tp * 0.01
                    + dd.qty2 * (dd.ori_rate + dd.float_rate)* cal_tp * 0.01 )as fore_duty_inout
            ,(dd.qty1 * (dd.actual_rate + dd.float_rate)* cal_tp * 0.01 
                    + dd.qty2 * (dd.ori_rate + dd.float_rate)* cal_tp* 0.01 )/ex_rate as fore_basicduty_inout
            ,0.13 * ((dd.qty1-dd.qty2) * (dd.actual_rate + dd.float_rate)* cal_tp * 0.01
                    + dd.qty2 * (dd.ori_rate + dd.float_rate)* cal_tp * 0.01)/ex_rate as vat
            ,1.13 * ((dd.qty1-dd.qty2) * (dd.actual_rate + dd.float_rate)* cal_tp * 0.01
                    + dd.qty2 * (dd.ori_rate + dd.float_rate)* cal_tp * 0.01 )/ex_rate as fore_duty_inout               
            ,date_format('2021-12-22','yyyy')              as dt_year
            ,date_format('2021-12-22','MM')                as dt_month
            ,date_format('2021-12-22','yyyy-MM-dd')        as dt
      from dd
;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : .................."