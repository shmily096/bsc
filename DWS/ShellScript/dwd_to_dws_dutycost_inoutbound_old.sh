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


with a_inbound as(
select 
    upn,
    case when month('${sync_date[day]}') ='1' then jan_inbound_qty
         when month('${sync_date[day]}') ='2' then feb_inbound_qty
         when month('${sync_date[day]}') ='3' then mar_inbound_qty
         when month('${sync_date[day]}') ='4' then apr_inbound_qty
         when month('${sync_date[day]}') ='5' then may_inbound_qty
         when month('${sync_date[day]}') ='6' then jun_inbound_qty
         when month('${sync_date[day]}') ='7' then jul_inbound_qty
         when month('${sync_date[day]}') ='8' then aug_inbound_qty
         when month('${sync_date[day]}') ='9' then sep_inbound_qty
         when month('${sync_date[day]}') ='10' then oct_inbound_qty
         when month('${sync_date[day]}') ='11' then nov_inbound_qty
         when month('${sync_date[day]}') ='12' then dec_inbound_qty
    else 0    
    end as inbound_qty
from ods_inbound_rawdata
where dt in (select max (dt) from ods_inbound_rawdata)
    )
, a_outbound as(
select 
    upn,
    case when month('${sync_date[day]}') ='1' then jan_outbound_qty
         when month('${sync_date[day]}') ='2' then feb_outbound_qty
         when month('${sync_date[day]}') ='3' then mar_outbound_qty
         when month('${sync_date[day]}') ='4' then apr_outbound_qty
         when month('${sync_date[day]}') ='5' then may_outbound_qty
         when month('${sync_date[day]}') ='6' then jun_outbound_qty
         when month('${sync_date[day]}') ='7' then jul_outbound_qty
         when month('${sync_date[day]}') ='8' then aug_outbound_qty
         when month('${sync_date[day]}') ='9' then sep_outbound_qty
         when month('${sync_date[day]}') ='10' then oct_outbound_qty
         when month('${sync_date[day]}') ='11' then nov_outbound_qty
         when month('${sync_date[day]}') ='12' then dec_outbound_qty
    else 0
    end as outbound_qty
from ods_outbound_rawdata
where dt in (select max (dt) from ods_outbound_rawdata)
)
, b_in as(
    select upn, inbound_qty 
    from a_inbound 
    where inbound_qty> 0
    group by upn, inbound_qty )
, b_out as(
    select upn, outbound_qty 
    from a_outbound 
    where outbound_qty> 0
    group by upn, outbound_qty )

, b_inbound as(
select
    b_in.upn, 
    b_in.inbound_qty,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.disvision_name,
    ddm.mid,
    max(ddm.standard_cost) as standard_cost,
    ddm.delivery_plant
left join 
(select 
    sap_upl_level4_name,
    sap_upl_level5_name,
    disvision_name,
    mid,
    standard_cost,
    delivery_plant
from dwt_dqmonitor_material 
where dt in (select max(dt) from dwt_dqmonitor_material)
group by sap_upl_level4_name,
    sap_upl_level5_name,                    
    disvision_name,
    mid,
    standard_cost) ddm
on b_in.upn = ddm.mid
group by
    b_in.upn, 
    b_in.inbound_qty,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.disvision_name,
    ddm.mid,
    ddm.delivery_plant
)
, b_outbound as(
select
    b_out.upn, 
    b_out.outbound_qty,
    ddm.sap_upl_level4_name,
    ddm.sap_upl_level5_name,
    ddm.disvision_name,
    ddm.mid,
    max(ddm.standard_cost) as standard_cost,
    ddm.delivery_plant
left join 
(select 
    sap_upl_level4_name,
    sap_upl_level5_name,
    disvision_name,
    mid,
    standard_cost,
    delivery_plant
from dwt_dqmonitor_material 
where dt in (select max(dt) from dwt_dqmonitor_material)
group by sap_upl_level4_name,
    sap_upl_level5_name,                    
    disvision_name,
    mid,
    standard_cost) ddm
on b_in.upn = ddm.mid
group by
    b_out.upn,  
    b_out.outbound_qty,
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
-- inbound 排除D835且分拨的qty
, c_inbound as( 
SELECT 
    b_inbound.upn, 
    b_inbound.inbound_qty,
    b_inbound.sap_upl_level4_name,
    b_inbound.sap_upl_level5_name,
    b_inbound.disvision_name,
    b_inbound.standard_cost,
    b_inbound.delivery_plant,
    ccm.distribution_properties,
    ccm.mfn_tax_rate,
    ccm.hs_code,
    ccm.unit_price,
    ccm.provisional_tax_rate,
    ccm.origin_country
from b_inbound 
left join ccm
on b_inbound.upn=ccm.material
where not (b_inbound.delivery_plant='D835' and ccm.distribution_properties='\u53ef\u5206\u62e8')
group by 
    b_inbound.upn, 
    b_inbound.inbound_qty,
    b_inbound.sap_upl_level4_name,
    b_inbound.sap_upl_level5_name,
    b_inbound.disvision_name,
    b_inbound.standard_cost,
    b_inbound.delivery_plant,
    ccm.distribution_properties,
    ccm.mfn_tax_rate,
    ccm.hs_code,
    ccm.unit_price,
    ccm.provisional_tax_rate,
    ccm.origin_country
)
--仅包含D835且分拨的qty
, c_outbound as( 
SELECT 
    b_outbound.upn, 
    b_outbound.outbound_qty,
    b_outbound.sap_upl_level4_name,
    b_outbound.sap_upl_level5_name,
    b_outbound.disvision_name,
    b_outbound.standard_cost,
    b_outbound.delivery_plant,
    ccm.distribution_properties,
    ccm.mfn_tax_rate,
    ccm.hs_code,
    ccm.unit_price,
    ccm.provisional_tax_rate,
    ccm.origin_country
from b_outbound 
left join ccm
on b_outbound.upn=ccm.material
where (b_outbound.delivery_plant='D835' and ccm.distribution_properties='\u53ef\u5206\u62e8')
group by
    b_outbound.upn, 
    b_outbound.outbound_qty,
    b_outbound.sap_upl_level4_name,
    b_outbound.sap_upl_level5_name,
    b_outbound.disvision_name,
    b_outbound.standard_cost,
    b_outbound.delivery_plant,
    ccm.distribution_properties,
    ccm.mfn_tax_rate,
    ccm.hs_code,
    ccm.unit_price,
    ccm.provisional_tax_rate,
    ccm.origin_country
)
,c as(
SELECT 
    c_inbound.upn as upn, 
    c_inbound.inbound_qty as qty,
    c_inbound.sap_upl_level4_name,
    c_inbound.sap_upl_level5_name,
    c_inbound.disvision_name,
    c_inbound.standard_cost,
    c_inbound.delivery_plant
    c_inbound.distribution_properties as distribution_properties,
    c_inbound.mfn_tax_rate as mfn_tax_rate,
    c_inbound.hs_code as hs_code,
    c_inbound.unit_price as unit_price,
    c_inbound.provisional_tax_rate as provisional_tax_rate,
    c_inbound.origin_country as origin_country,
    'in' as flag_in_out
from c_inbound 
union all
SELECT 
    c_outbound.upn as upn, 
    c_outbound.outbound_qty as qty,
    c_outbound.sap_upl_level4_name,
    c_outbound.sap_upl_level5_name,
    c_outbound.disvision_name,
    c_outbound.standard_cost,
    c_outbound.delivery_plant,
    c_outbound.distribution_properties as distribution_properties,
    c_outbound.mfn_tax_rate as mfn_tax_rate,
    c_outbound.hs_code as hs_code,
    c_outbound.unit_price as unit_price,
    c_outbound.provisional_tax_rate as provisional_tax_rate,
    c_outbound.origin_country as origin_country,
    'out' as flag_in_out
from c_outbound 
)
, d_saving as (
select 
    c.upn 
    ,c.qty
    ,c.sap_upl_level4_name
    ,c.sap_upl_level5_name
    ,c.disvision_name
    ,c.standard_cost
    ,c.delivery_plant
    ,c.distribution_properties
    ,c.mfn_tax_rate
    ,c.hs_code
    ,c.unit_price
    ,c.provisional_tax_rate
    ,c.origin_country
    ,c.flag_in_out
    ,sdl.saving_duty_rate   
from c left join 
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
from ods_saving_duty_list 
where dt in (select max(dt) from ods_saving_duty_list)) sdl
on c.hs_code = sdl.hs_code and c.origin_country = sdl.coo
)
, d as (
select 
    d_saving.upn 
    ,d_saving.qty
    ,d_saving.sap_upl_level4_name
    ,d_saving.sap_upl_level5_name
    ,d_saving.disvision_name
    ,d_saving.standard_cost
    ,d_saving.delivery_plant
    ,d_saving.distribution_properties
    ,d_saving.mfn_tax_rate
    ,d_saving.hs_code
    ,d_saving.unit_price
    ,d_saving.provisional_tax_rate
    ,d_saving.origin_country
    ,d_saving.flag_in_out
    ,d_saving.saving_duty_rate
    ,adl.add_on_rate
from d_saving 
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
    end as add_on_rate
from ods_add_on_duty_list 
where dt in (select max(dt) from ods_add_on_duty_list)) adl
on d_saving.hs_code = adl.hs_code and d_saving.origin_country = adl.coo
)
--coo
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
,ddd as(
select 
    d.upn 
    ,d.qty
    ,d.sap_upl_level4_name
    ,d.sap_upl_level5_name
    ,d.disvision_name
    ,d.standard_cost
    ,d.delivery_plant
    ,d.distribution_properties
    ,d.mfn_tax_rate
    ,d.hs_code
    ,d.unit_price
    ,d.provisional_tax_rate
    ,coalesce(d.provisional_tax_rate, d.mfn_tax_rate) as act_rate
    ,d.origin_country
    ,d.flag_in_out
    ,d.saving_duty_rate
    ,d.add_on_rate
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
    material) tvr 
on d.upn = tvr.material
group by 
    d.upn 
    ,d.qty
    ,d.sap_upl_level4_name
    ,d.sap_upl_level5_name
    ,d.disvision_name
    ,d.standard_cost
    ,d.delivery_plant
    ,d.distribution_properties
    ,d.mfn_tax_rate
    ,d.hs_code
    ,d.unit_price
    ,d.provisional_tax_rate
    ,d.origin_country
    ,d.flag_in_out
    ,d.saving_duty_rate
    ,d.add_on_rate
    ,tvr.transfer_price
 )
,dd as
(select
    ddd.upn 
    ,ddd.qty
    ,ddd.sap_upl_level4_name
    ,ddd.sap_upl_level5_name
    ,ddd.disvision_name
    ,ddd.standard_cost
    ,ddd.delivery_plant
    ,ddd.distribution_properties
    ,ddd.mfn_tax_rate
    ,ddd.hs_code
    ,ddd.unit_price
    ,ddd.provisional_tax_rate
    ,ddd.act_rate
    ,ddd.origin_country
    ,ddd.flag_in_out
    ,ddd.saving_duty_rate
    ,ddd.add_on_rate
    ,ddd.transfer_price
    ,coo.coo_rate
    ,case when coo.coo_rate ='0' then '0'
     else ddd.act_rate 
     end as actual_rate
from ddd 
left join coo
on ddd.upn = coo.upn
)


-- vat = 0.13* forecast duty inout
-- forecast duty= qty * (coalesce(provisional_rate, mfn)+加征-减免)*coalesce(TP,ctm:unit_price,mm:standard_cost*1.46)
insert overwrite table dws_dutycost_inoutbound partition(dt_year, dt_month, dt)
      select dd.upn
            ,dd.hs_code
            ,dd.qty
            ,dd.delivery_plant 
            ,dd.distribution_properties
            ,dd.origin_country
            ,dd.sap_upl_level4_name
            ,dd.sap_upl_level5_name
            ,dd.disvision_name
            ,dd.flag_in_out
            ,dd.transfer_price
            ,dd.unit_price
            ,dd.standard_cost
            ,coalesce(dd.transfer_price, dd.unit_price, dd.standard_cost*1.46) as cal_tp
            ,dd.provisional_tax_rate
            ,dd.mfn_tax_rate
            ,dd.coo_rate
            ,dd.actual_rate as duty_rate
            ,dd.saving_duty_rate
            ,dd.add_on_rate
            ,dd.qty * (dd.actual_rate + dd.add_on_rate - dd.saving_duty_rate)
                    * coalesce(dd.transfer_price, dd.unit_price, dd.standard_cost*1.46) as fore_basicduty_inout
            ,0.13 * (dd.qty * (dd.actual_rate + dd.add_on_rate - dd.saving_duty_rate)
                    * coalesce(dd.transfer_price, dd.unit_price, dd.standard_cost*1.46) as vat
            ,1.13 * （dd.qty * (dd.actual_rate + dd.add_on_rate - dd.saving_duty_rate)
                    * coalesce(dd.transfer_price, dd.unit_price, dd.standard_cost*1.46) as fore_duty_inout
            ,date_format('${sync_date[day]}','yyyy')              as dt_year
            ,date_format('${sync_date[day]}','MM')                as dt_month
            ,date_format('${sync_date[day]}','yyyy-MM-dd')        as dt
      from dd

;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : .................."