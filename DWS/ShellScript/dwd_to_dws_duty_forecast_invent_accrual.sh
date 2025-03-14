#!/bin/bash
# Function:
#   sync up xxxx 
# History:
# 2021-11-25    Amanda   v1.0    init

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


with a as(
select 
    upn,
    case when month('2021-12-22') ='1' then Jan_qty
         when month('2021-12-22') ='2' then Feb_qty
         when month('2021-12-22') ='3' then Mar_qty
         when month('2021-12-22') ='4' then Apr_qty
         when month('2021-12-22') ='5' then May_qty
         when month('2021-12-22') ='6' then Jun_qty
         when month('2021-12-22') ='7' then Jul_qty
         when month('2021-12-22') ='8' then Aug_qty
         when month('2021-12-22') ='9' then Sep_qty
         when month('2021-12-22') ='10' then Oct_qty
         when month('2021-12-22') ='11' then Nov_qty
         when month('2021-12-22') ='12' then Dec_qty
    else 0    
    end as inv_qty
from dwd_fact_inventory_rawdata
where dt in (select max (dt) from dwd_fact_inventory_rawdata)
    )
, aa as(
    select upn, inv_qty 
    from a 
    where inv_qty> 0
    group by upn, inv_qty )
, buf as ( 
select upn, 'Y' as buffer_flag
from dwd_dim_buffer_hold_list
where dt in (select max(dt) from dwd_dim_buffer_hold_list)
)
--, buf_AOP as (
--select
--    upn,
--    case when month('2021-12-22') ='1' then Jan
--         when month('2021-12-22') ='2' then Feb
--         when month('2021-12-22') ='3' then Mar
--         when month('2021-12-22') ='4' then Apr
--         when month('2021-12-22') ='5' then May
--         when month('2021-12-22') ='6' then Jun
--        when month('2021-12-22') ='7' then Jul
--         when month('2021-12-22') ='8' then Aug
--         when month('2021-12-22') ='9' then Sep
--         when month('2021-12-22') ='10' then Oct
--         when month('2021-12-22') ='11' then Nov
--         when month('2021-12-22') ='12' then Dec
--    else 0    
--    end as buffer_qty
--from
--    dwd_dim_buffer_list
--where dt in ( select max(dt) from dwd_dim_buffer_list)
--    and dt_year = year('2021-12-21')
--    and vaild_to >= '2021-12-21' )
, a_inv as(
select 
    aa.upn, 
    if(aa.inv_qty is null, 0, aa.inv_qty) as inv_qty,
    buf.buffer_flag
from aa 
left join buf
on aa.upn = buf.upn
)

,ddm as (
SELECT 
    sap_upl_level4_name,
    sap_upl_level5_name,
    division_display_name,
    mit,
    cast(standard_cost as decimal(9,2)) as standard_cost,
    delivery_plant
from
    dwd_dim_material inter
where
    dt in (select max(dt) from dwd_dim_material)
group by
    sap_upl_level4_name,
    sap_upl_level5_name,
    division_display_name,
    mit,
    standard_cost,
    delivery_plant)
, b as (		
select 
    a_inv.upn,
    a_inv.inv_qty,
    a_inv.buffer_flag,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.division_display_name,
    max(ddm.standard_cost) as standard_cost,
    ddm.delivery_plant	
from a_inv
left join ddm
on ARRAY_CONTAINS(ddm.mit, a_inv.upn)
group by
    a_inv.upn,
    a_inv.inv_qty,
    a_inv.buffer_flag,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.division_display_name,
    ddm.delivery_plant
)
,ex_ as(
    select rate, from_currency, valid_from 
     from dwd_dim_exchange_rate
    where from_currency='USD' and to_currency ='CNY'
    and dt in (select max(dt) from dwd_dim_exchange_rate) )
,ex_rate as(
    select cast(rate as decimal(9,2)) as rate, from_currency
    from ex_ where valid_from = (select max(valid_from) from ex_) )
,cm as (
select 
    distribution_properties,
    mfn_tax_rate,
    hs_code,
    material,
    unit_price,
    provisional_tax_rate,
    origin_country,
    currency
from dwd_ctm_customer_master
where dt in (select max(dt) from dwd_ctm_customer_master)
group by 
    distribution_properties,
    mfn_tax_rate,
    hs_code,
    material,
    unit_price,
    provisional_tax_rate,
    origin_country,
    currency
)
,cmm as (
select 
    cm.distribution_properties,
    cm.mfn_tax_rate,
    cm.hs_code,
    cm.material,
    if(ex_rate.rate is null,cm.unit_price ,cm.unit_price/ex_rate.rate) as unit_price,
    'USD' as currency,
    cm.provisional_tax_rate,
    cm.origin_country 
from cm
left join ex_rate
on cm.currency = ex_rate.from_currency
)
,ccm as (
select 
    cmm.distribution_properties,
    cmm.mfn_tax_rate,
    cmm.hs_code,
    cmm.material,
    cmm.unit_price,
    ex_rate.rate as ex_rate,
    cmm.provisional_tax_rate,
    cmm.origin_country 
from cmm
left join ex_rate
on cmm.currency = ex_rate.from_currency
)
--D835且分拨的 cal_rate 1; D838的 & D835非分拨 cal_rate 0.15
,c as (
SELECT 
    b.upn
    ,b.inv_qty
    ,b.buffer_flag
    ,b.sap_upl_level4_name
    ,b.sap_upl_level5_name
    ,b.division_display_name
    ,b.standard_cost
    ,b.delivery_plant
    ,ccm.distribution_properties
    ,ccm.mfn_tax_rate
    ,ccm.hs_code
    ,ccm.unit_price
    ,ccm.provisional_tax_rate
    ,ccm.origin_country
    ,ccm.ex_rate
    ,case when b.delivery_plant='D835' and ccm.distribution_properties='可分拨' then 1
          when b.delivery_plant='D835' and ccm.distribution_properties='不可分拨' then 0.15
          when b.delivery_plant='D838' then 0.15
     else 0
     end as cal_rate
from b left join ccm
on b.upn = ccm.material
)

, d as (
select 
    c.upn
    ,c.inv_qty
    ,c.buffer_flag
    ,c.sap_upl_level4_name
    ,c.sap_upl_level5_name
    ,c.division_display_name
    ,c.standard_cost
    ,c.delivery_plant
    ,c.distribution_properties
    ,c.mfn_tax_rate
    ,c.hs_code
    ,c.unit_price
    ,c.provisional_tax_rate
    ,c.origin_country
    ,c.ex_rate
    ,c.cal_rate
    ,if(adl.add_on_rate is null, 0, adl.add_on_rate) as add_on_rate
from c 
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
on c.hs_code = adl.hs_code and c.origin_country = adl.country_code
)
,e as(
select
    d.upn
    ,d.inv_qty
    ,d.buffer_flag
    ,d.sap_upl_level4_name
    ,d.sap_upl_level5_name
    ,d.division_display_name
    ,d.standard_cost
    ,d.delivery_plant
    ,d.distribution_properties
    ,d.mfn_tax_rate
    ,d.hs_code
    ,d.unit_price
    ,d.provisional_tax_rate
    ,d.origin_country
    ,d.ex_rate
    ,d.cal_rate
    ,d.add_on_rate
    ,if(coalesce(d.provisional_tax_rate, d.mfn_tax_rate) is null, 0, coalesce(d.provisional_tax_rate, d.mfn_tax_rate)) as ori_rate
    ,tvr.transfer_price  
from d
left join 
(select 
    transfer_price, material
from dwd_fact_tp_validation_report 
where dt in (select max(dt) from dwd_fact_tp_validation_report )
group by 
    transfer_price, material
)tvr 
on d.upn = tvr.material    
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
,ocl as
(select 
    upn, coo, qty
from dwd_dim_original_change
where dt ='2021-12-22' and vaild_to >= '2021-12-22'
)
,dcl AS 
(select
    upn, coo, reason_code, duty_change_rate
from dwd_dim_duty_change
where dt ='2021-12-22' and vaild_to >= '2021-12-22'
)
--accrual_duty = qty*coalese(tp, ctm: unit_price, mm:standard_cosr*1.46)*(coalesce(d.provisional_tax_rate, d.mfn_tax_rate)  + 加征 )*cal_rate
,ee as (
select 
    e.upn
    ,e.inv_qty
    ,e.buffer_flag
    ,ocl.qty as qty2
    ,e.distribution_properties
    ,e.mfn_tax_rate
    ,e.hs_code
    ,e.unit_price
    ,coo.fta_country
    ,e.origin_country
    ,e.ex_rate
    ,e.cal_rate
    ,e.add_on_rate
    ,e.provisional_tax_rate
    ,e.ori_rate
    ,coo.coo_rate
    ,(e.ori_rate + coo.coo_rate)as actual_rate
    ,e.sap_upl_level4_name
    ,e.sap_upl_level5_name
    ,e.division_display_name
    ,e.standard_cost
    ,e.delivery_plant
    ,e.transfer_price
    ,dcl.reason_code
    ,if(dcl.duty_change_rate is null, 0,dcl.duty_change_rate) as duty_change_rate
    ,coalesce(e.transfer_price, e.unit_price, e.standard_cost*1.46) as cal_tp
from e
left join coo
on e.upn = coo.upn
left join  ocl
on e.upn = ocl.upn and e.origin_country = ocl.coo
left join  dcl
on e.upn =dcl.upn and e.origin_country = dcl.coo
)
,eed as(
select 
    upn
    ,inv_qty
    ,buffer_flag
    ,if (qty2 is null, 0, qty2) as qty2
    ,distribution_properties
    ,mfn_tax_rate
    ,hs_code
    ,unit_price
    ,origin_country
    ,ex_rate
    ,fta_country
    ,cast(cal_rate as decimal(9,2)) as cal_rate
    ,add_on_rate
    ,provisional_tax_rate
    ,ori_rate
    ,coo_rate
    ,cast(actual_rate as decimal(18,2)) as actual_rate
    ,sap_upl_level4_name
    ,sap_upl_level5_name
    ,division_display_name
    ,standard_cost
    ,delivery_plant
    ,transfer_price
    ,reason_code
    ,duty_change_rate
    ,cal_tp
    ,(add_on_rate + duty_change_rate) as float_rate
from ee
)


insert overwrite table dws_duty_forecast__invent_accrual partition(dt_year, dt_month,dt)
select 
    upn
    ,hs_code
    ,inv_qty
    ,qty2
    ,buffer_flag
    ,delivery_plant
    ,distribution_properties
    ,origin_country
    ,ex_rate
    ,sap_upl_level4_name
    ,sap_upl_level5_name
    ,division_display_name
    ,reason_code
    ,fta_country
    ,transfer_price
    ,unit_price
    ,standard_cost
    ,cal_tp
    ,provisional_tax_rate
    ,mfn_tax_rate
    ,ori_rate
    ,coo_rate
    ,actual_rate
    ,cal_rate
    ,add_on_rate
    ,duty_change_rate
    ,float_rate
    ,if(buffer_flag="Y", 0
                       ,(cal_tp * (inv_qty-qty2)* (actual_rate + float_rate) * cal_rate * 0.01
                        + cal_tp * qty2 *(ori_rate + float_rate )* 0.01)) as fore_duty_invent
    ,if(buffer_flag="Y", 0
                       ,(cal_tp * (inv_qty-qty2)* (actual_rate + float_rate) * cal_rate * 0.01
                        + cal_tp * qty2 *(ori_rate + float_rate )* 0.01)/ex_rate) as fore_duty_invent
    ,date_format('2021-12-24','yyyy')              as dt_year
    ,date_format('2021-12-24','MM')                as dt_month
    ,date_format('2021-12-24','yyyy-MM-dd')        as dt
from eed
    ;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : .................."