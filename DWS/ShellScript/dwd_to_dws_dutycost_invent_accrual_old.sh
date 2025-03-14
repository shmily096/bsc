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
    storage_location,
    case when month('${sync_date[day]}') ='1' then jan_inv_qty
         when month('${sync_date[day]}') ='2' then feb_inv_qty
         when month('${sync_date[day]}') ='3' then mar_inv_qty
         when month('${sync_date[day]}') ='4' then apr_inv_qty
         when month('${sync_date[day]}') ='5' then may_inv_qty
         when month('${sync_date[day]}') ='6' then jun_inv_qty
         when month('${sync_date[day]}') ='7' then jul_inv_qty
         when month('${sync_date[day]}') ='8' then aug_inv_qty
         when month('${sync_date[day]}') ='9' then sep_inv_qty
         when month('${sync_date[day]}') ='10' then oct_inv_qty
         when month('${sync_date[day]}') ='11' then nov_inv_qty
         when month('${sync_date[day]}') ='12' then dec_inv_qty
    else 0    
    end as inv_qty
from ods_inventory_rawdata
where dt in (select max (dt) from ods_inventory_rawdata)
    )
, aa as(
    select upn, inv_qty 
    from a 
    where inv_qty> 0
    group by upn, storage_location, inv_qty )
, a_inv as(
select 
    upn, 
    inv_qty - buffer_qty as inv_qty-----------------------------------------------
from aa 
left join buffer
on
)
, b as (
select
    a_inv.upn, 
    a_inv.inv_qty,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.disvision_name,
    ddm.mid,
    max(ddm.standard_cost) as standard_cost,
    ddm.delivery_plant
from a_inv
left join 
(select 
    sap_upl_level4_name,
    sap_upl_level5_name,
    disvision_name,
    mid,
    standard_cost
from dwt_dqmonitor_material 
where dt in (select max(dt) from dwd_dim_material)
group by 
    sap_upl_level4_name,
    sap_upl_level5_name,                    
    disvision_name,
    mid,
    standard_cost) ddm
on a_inv.upn = ddm.mid
group by
    a_inv.upn, 
    a_inv.inv_qty,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.disvision_name,
    ddm.mid,
    ddm.delivery_plant
)
,ex_ as(
    select rate, from_currency, valid_from 
     from dwd_dim_exchange_rate
    where from_currency='USD' and to_currency ='CNY'
    and dt in (select max(dt) from dwd_dim_exchange_rate) )
,ex_rate as(
    select rate, from_currency
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
,ccm as (
select 
    cm.distribution_properties,
    cm.mfn_tax_rate,
    cm.hs_code,
    cm.material,
    cm.unit_price/ex_rate.rate as unit_price,
    cm.provisional_tax_rate,
    cm.origin_country 
from cm
left join ex_rate
on cm.currency = ex_rate.from_currency
)


--D835且分拨的 cal_rate 1; D838的 & D835非分拨 cal_rate 0.15
,c as (
SELECT 
    b.upn 
    ,b.inv_qty
    ,b.sap_upl_level4_name,
    ,b.sap_upl_level5_name,
    ,b.disvision_name,
    ,b.mid,
    ,b.standard_cost,
    ,b.delivery_plant
    ,ccm.distribution_properties
    ,ccm.mfn_tax_rate
    ,ccm.hs_code
    ,ccm.unit_price
    ,ccm.provisional_tax_rate
    ,ccm.original_country
    ,case when b.delivery_plant='D835' and ccm.distribution_properties='\u5206\u62e8' then 1
          when b.delivery_plant='D835' and ccm.distribution_properties='\u53ef\u5206\u62e8' then 0.15
          when b.delivery_plant='D838' then 0.15
     else 0
     end as cal_rate
from b left join ccm
on b.upn = ccm.material
where not (b.delivery_plant='D835' and ccm.distribution_properties='\u4e0d\u53ef\u5206\u62e8')
)
, d as (
select 
    c.upn 
    ,c.inv_qty
    ,c.sap_upl_level4_name
    ,c.sap_upl_level5_name
    ,c.disvision_name
    ,c.mid
    ,c.standard_cost
    ,c.delivery_plant
    ,c.distribution_properties
    ,c.mfn_tax_rate
    ,c.hs_code
    ,c.unit_price
    ,c.provisional_tax_rate
    ,c.original_country
    ,c.cal_rate
    ,adl.add_on_rate
from c 
left join 
(select 
    hs_code,
    coo,
    case when month('${sync_date[day]}') ='1' then jan
         when month('${sync_date[day]}') ='2' then feb
         when month('${sync_date[day]}') ='3' then mar
         when month('${sync_date[day]}') ='4' then apr
         when month('${sync_date[day]}') ='5' then may
         when month('${sync_date[day]}') ='6' then jun
         when month('${sync_date[day]}') ='7' then jul
         when month('${sync_date[day]}') ='8' then aug
         when month('${sync_date[day]}') ='9' then sep
         when month('${sync_date[day]}') ='10' then oct
         when month('${sync_date[day]}') ='11' then nov
         when month('${sync_date[day]}') ='12' then dec
    else 0    
    end as saving_duty_rate
from ods_add_on_duty_list 
where dt in (select max(dt) from ods_add_on_duty_list)) adl
on c.hs_code = adl.hs_code and c.original_country = adl.coo
)
--coo_list
,coo as(
select 
    UPN,
    case when month('${sync_date[day]}') ='1' then jan
         when month('${sync_date[day]}') ='2' then feb
         when month('${sync_date[day]}') ='3' then mar
         when month('${sync_date[day]}') ='4' then apr
         when month('${sync_date[day]}') ='5' then may
         when month('${sync_date[day]}') ='6' then jun
         when month('${sync_date[day]}') ='7' then jul
         when month('${sync_date[day]}') ='8' then aug
         when month('${sync_date[day]}') ='9' then sep
         when month('${sync_date[day]}') ='10' then oct
         when month('${sync_date[day]}') ='11' then nov
         when month('${sync_date[day]}') ='12' then dec
    else 0    
    end as coo_rate
from ods_coo_duty_list
)
,e as(
select
    d.upn
    ,d.inv_qty
    ,d.sap_upl_level4_name,
    ,d.sap_upl_level5_name,
    ,d.disvision_name,
    ,d.mid,
    ,d.standard_cost,
    ,d.delivery_plant
    ,d.distribution_properties
    ,d.mfn_tax_rate
    ,d.hs_code
    ,d.unit_price
    ,d.provisional_tax_rate
    ,d.original_country
    ,d.cal_rate
    ,d.add_on_rate
    ,coalesce(d.provisional_tax_rate, d.mfn_tax_rate) as act_duty
    ,tvr.transfer_price  
from d
left join 
(select 
    transfer_price,
    material
from ods_tp_vaildation_report 
where dt in (select max(dt) from ods_tp_vaildation_report )
group by 
    transfer_price,
    material
)tvr 
on d.upn = tvr.material
group by 
    d.upn 
    ,d.inv_qty
    ,d.sap_upl_level4_name,
    ,d.sap_upl_level5_name,
    ,d.disvision_name,
    ,d.mid,
    ,d.standard_cost,
    ,d.delivery_plant
    ,d.distribution_properties
    ,d.mfn_tax_rate
    ,d.hs_code
    ,d.unit_price
    ,d.cal_rate
    ,d.add_on_rate
    ,tvr.transfer_price    
   )
--accrual_duty = qty*coalese(tp, ctm: unit_price, mm:standard_cosr*1.46)*(coalesce(d.provisional_tax_rate, d.mfn_tax_rate)  + 加征 )*cal_rate
,ee as (
select 
    e.upn 
    ,e.inv_qty
    ,ocl.qty as qty2
    ,e.distribution_properties
    ,e.mfn_tax_rate
    ,e.hs_code
    ,e.unit_price
    ,e.original_country
    ,e.cal_rate
    ,e.add_on_rate
    ,e.provisional_tax_rate
    ,e.act_rate as ori_rate
    ,case when coo.coo_rate ='0' then '0'
     else e.act_rate 
     end as actual_rate
    ,e.sap_upl_level4_name
    ,e.sap_upl_level5_name
    ,e.disvision_name
    ,e.standard_cost
    ,e.delivery_plant
    ,e.transfer_price
    ,dcl.reason_code
    ,dcl.duty_change_rate
    ,coalesce(e.transfer_price, e.unit_price, e.standard_cost*1.46) as cal_tp
    ,e.add_on_rate + dcl.duty_change_rate as float_rate
from e
left join coo
on e.upn = coo.upn
left join original_country_list ocl
on e.upn = ocl.upn
left join duty_change_list dcl
on e.upn =dcl.upn
)


insert overwrite table dws_dutycost_invent_accrual partition(dt_year, dt_month,dt)
         select 
            upn
            ,hs_code
            ,inv_qty
            ,qty2a
            ,delivery_plant 
            ,distribution_properties
            ,original_country
            ,sap_upl_level4_name
            ,sap_upl_level5_name
            ,disvision_name
            ,reason_code
            ,transfer_price
            ,unit_price
            ,standard_cost
            ,cal_tp
            ,provisional_tax_rate
            ,mfn_tax_rate
            ,coo_rate
            ,ori_rate
            ,actual_rate
            ,add_on_rate
            ,duty_change_rate
            ,float_rate
            ,cal_rate
            ,cal_tp * （inv_qty-qty2）* (actual_rate + float_rate) * cal_rate
                     + cal_tp * qty2 *(ori_rate + float_rate)  as fore_duty_invent
            ,date_format('${sync_date[day]}','yyyy')              as dt_year
            ,date_format('${sync_date[day]}','MM')                as dt_month
            ,date_format('${sync_date[day]}','yyyy-MM-dd')                as dt
          from ee


;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : .................."