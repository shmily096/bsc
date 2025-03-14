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
	case
		when month('2021-12-21') = '1' then Jan_qty
		when month('2021-12-21') = '2' then Feb_qty
		when month('2021-12-21') = '3' then Mar_qty
		when month('2021-12-21') = '4' then Apr_qty
		when month('2021-12-21') = '5' then May_qty
		when month('2021-12-21') = '6' then Jun_qty
		when month('2021-12-21') = '7' then Jul_qty
		when month('2021-12-21') = '8' then Aug_qty
		when month('2021-12-21') = '9' then Sep_qty
		when month('2021-12-21') = '10' then Oct_qty
		when month('2021-12-21') = '11' then Nov_qty
		when month('2021-12-21') = '12' then Dec_qty
		else 0
	end as inbound_qty
from
	dwd_fact_inbound_rawdata
where
	dt in (select max (dt) from dwd_fact_inbound_rawdata) ) 
,a_outbound as(
select
	upn,
	case
		when month('2021-12-21') = '1' then Jan_qty
		when month('2021-12-21') = '2' then Feb_qty
		when month('2021-12-21') = '3' then Mar_qty
		when month('2021-12-21') = '4' then Apr_qty
		when month('2021-12-21') = '5' then May_qty
		when month('2021-12-21') = '6' then Jun_qty
		when month('2021-12-21') = '7' then Jul_qty
		when month('2021-12-21') = '8' then Aug_qty
		when month('2021-12-21') = '9' then Sep_qty
		when month('2021-12-21') = '10' then Oct_qty
		when month('2021-12-21') = '11' then Nov_qty
		when month('2021-12-21') = '12' then Dec_qty
		else 0
	end as outbound_qty
from
	dwd_fact_outbound_rawdata
where
	dt in ( select max (dt) from dwd_fact_outbound_rawdata) ) 
,b_in as(
select upn, inbound_qty
from a_inbound
where inbound_qty> 0
group by upn, inbound_qty ) 
,b_out as(
select upn, outbound_qty
from a_outbound
where outbound_qty> 0
group by upn, outbound_qty )
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
	dt in (select max(dt) from dwd_dim_material )
group by
	sap_upl_level4_name,
	sap_upl_level5_name,
	division_display_name,
	mit,
	standard_cost,
	delivery_plant)
, bb_in(		
select 
    b_in.upn,
    b_in.inbound_qty,
    ddm.sap_upl_level4_name,
	ddm.sap_upl_level5_name,
	ddm.division_display_name,
	max(ddm.standard_cost) as standard_cost,
	ddm.delivery_plant	
from b_in
left join ddm
on ARRAY_CONTAINS(ddm.mit, b_in.upn)
group BY 
    b_in.upn,
    b_in.inbound_qty,
    ddm.sap_upl_level4_name,
	ddm.sap_upl_level5_name,
	ddm.division_display_name,
	ddm.delivery_plant)
, buf as ( 
select upn, 'Y' as buffer_flag
from dwd_dim_buffer_hold_list
where dt in (select max(dt) from dwd_dim_buffer_hold_list)
)
--, buf_AOP as (
--select
--	upn,
--	flag as buffer_flag
--from
--	dwd_dim_buffer_list
--where dt in ( select max(dt) from dwd_dim_buffer_list)
--	and dt_year = year('2021-12-21')
--	and vaild_to >= '2021-12-21'
--group BY
--	upn,
--	flag )
, b_inbound as (
select 
    bb_in.upn,
    bb_in.inbound_qty,
    bb_in.sap_upl_level4_name,
	bb_in.sap_upl_level5_name,
	bb_in.division_display_name,
	bb_in.standard_cost,
	bb_in.delivery_plant,
	buf.buffer_flag
from bb_in		
left join 
	 buf 
on bb_in.upn = buf.upn
group by
	bb_in.upn,
    bb_in.inbound_qty,
    bb_in.sap_upl_level4_name,
	bb_in.sap_upl_level5_name,
	bb_in.division_display_name,
	bb_in.standard_cost,
	bb_in.delivery_plant,
	buf.buffer_flag )
, bb_out(		
select 
    b_out.upn,
    b_out.outbound_qty,
    ddm.sap_upl_level4_name,
	ddm.sap_upl_level5_name,
	ddm.division_display_name,
	max(ddm.standard_cost) as standard_cost,
	ddm.delivery_plant	
from b_out
left join ddm
on ARRAY_CONTAINS(ddm.mit, b_out.upn)
group BY 
    b_out.upn,
    b_out.outbound_qty,
    ddm.sap_upl_level4_name,
	ddm.sap_upl_level5_name,
	ddm.division_display_name,
	ddm.delivery_plant)
,b_outbound as (
select 
    bb_out.upn,
    bb_out.outbound_qty,
    bb_out.sap_upl_level4_name,
	bb_out.sap_upl_level5_name,
	bb_out.division_display_name,
	bb_out.standard_cost,
	bb_out.delivery_plant,
	buf.buffer_flag
from bb_out		
left join 
	 buf 
on bb_out.upn = buf.upn
group by
	bb_out.upn,
    bb_out.outbound_qty,
    bb_out.sap_upl_level4_name,
	bb_out.sap_upl_level5_name,
	bb_out.division_display_name,
	bb_out.standard_cost,
	bb_out.delivery_plant,
	buf.buffer_flag )
, ex_ as(
select
	cast(rate as decimal(9,2)),
	from_currency,
	valid_from
from
	dwd_dim_exchange_rate
where
	from_currency = 'USD'
	and to_currency = 'CNY'
	and dt in (select max(dt) from dwd_dim_exchange_rate) )
,ex_rate as(
select
	rate,
	from_currency
from
	ex_
where valid_from = (select max(valid_from) from ex_) ) 
, cm as (
select
	distribution_properties,
	mfn_tax_rate,
	hs_code,
	material,
	unit_price,
	provisional_tax_rate,
	origin_country,
	currency
from
	dwd_ctm_customer_master
	where
dt in (select max(dt) from dwd_ctm_customer_master)
group by
	distribution_properties,
	mfn_tax_rate,
	hs_code,
	material,
	unit_price,
	provisional_tax_rate,
	origin_country,
	currency ) 
,cmm as (
select
	cm.distribution_properties,
	cm.mfn_tax_rate,
	cm.hs_code,
	cm.material,
	if(ex_rate.rate is null, cm.unit_price, cm.unit_price / ex_rate.rate) as unit_price,
	'USD' as currency,
	cm.provisional_tax_rate,
	cm.origin_country
from
	cm
left join ex_rate
on cm.currency = ex_rate.from_currency )
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
from
	cmm
left join ex_rate
on cmm.currency = ex_rate.from_currency )

-- inbound (D835+not buffer+not分拨) & （D838+not buffer)
,c_inbound as(
SELECT
	b_inbound.upn,
	b_inbound.inbound_qty,
	b_inbound.sap_upl_level4_name,
	b_inbound.sap_upl_level5_name,
	b_inbound.division_display_name,
	b_inbound.standard_cost,
	b_inbound.delivery_plant,
	b_inbound.buffer_flag,
	ccm.distribution_properties,
	ccm.mfn_tax_rate,
	ccm.hs_code,
	ccm.unit_price,
	ccm.provisional_tax_rate,
	ccm.origin_country,
	ccm.ex_rate
from b_inbound
left join ccm
on b_inbound.upn = ccm.material
where
(
	b_inbound.delivery_plant = 'D835'
    and ccm.distribution_properties = '不可分拨' --'\u53ef\u5206\u62e8'
	and b_inbound.buffer_flag is null
	)
	or (b_inbound.delivery_plant = 'D838'
	and b_inbound.buffer_flag is null)
group by
	b_inbound.upn,
	b_inbound.inbound_qty,
	b_inbound.sap_upl_level4_name,
	b_inbound.sap_upl_level5_name,
	b_inbound.division_display_name,
	b_inbound.standard_cost,
	b_inbound.delivery_plant,
	b_inbound.buffer_flag,
	ccm.distribution_properties,
	ccm.mfn_tax_rate,
	ccm.hs_code,
	ccm.unit_price,
	ccm.provisional_tax_rate,
	ccm.origin_country,
	ccm.ex_rate 
)
--outbound (D835+is buffer)&(D835+not buffer+分拨)&(D838+is buffer)------------------------------------
,c_outbound as(
SELECT
	b_outbound.upn,
	b_outbound.outbound_qty,
	b_outbound.sap_upl_level4_name,
	b_outbound.sap_upl_level5_name,
	b_outbound.division_display_name,
	b_outbound.standard_cost,
	b_outbound.delivery_plant,
	b_outbound.buffer_flag,
	ccm.distribution_properties,
	ccm.mfn_tax_rate,
	ccm.hs_code,
	ccm.unit_price,
	ccm.provisional_tax_rate,
	ccm.origin_country,
	ccm.ex_rate
from b_outbound
left join ccm 
on b_outbound.upn = ccm.material
where
	(b_outbound.delivery_plant = 'D835'
	and b_outbound.buffer_flag ="Y")
	or (b_outbound.delivery_plant = 'D835'
	and b_outbound.buffer_flag is null
	and ccm.distribution_properties = '可分拨')
	or (b_outbound.delivery_plant = 'D838'
	and b_outbound.buffer_flag ="Y" )
group by
	b_outbound.upn,
	b_outbound.outbound_qty,
	b_outbound.sap_upl_level4_name,
	b_outbound.sap_upl_level5_name,
	b_outbound.division_display_name,
	b_outbound.standard_cost,
	b_outbound.delivery_plant,
	b_outbound.buffer_flag,
	ccm.distribution_properties,
	ccm.mfn_tax_rate,
	ccm.hs_code,
	ccm.unit_price,
	ccm.provisional_tax_rate,
	ccm.origin_country,
	ccm.ex_rate ) 
	
	
	
insert overwrite table dws_duty_inbound_outbound_mid partition(dt)
SELECT
	c_inbound.upn as upn,
	c_inbound.inbound_qty as qty,
	c_inbound.sap_upl_level4_name,
	c_inbound.sap_upl_level5_name,
	c_inbound.division_display_name,
	c_inbound.standard_cost,
	c_inbound.delivery_plant,
	c_inbound.buffer_flag,
	c_inbound.distribution_properties as distribution_properties,
	c_inbound.mfn_tax_rate as mfn_tax_rate,
	c_inbound.hs_code as hs_code,
	c_inbound.unit_price as unit_price,
	c_inbound.provisional_tax_rate as provisional_tax_rate,
	c_inbound.origin_country as origin_country,
	c_inbound.ex_rate,
	'inbound' as flag_in_out,
	'2021-12-24' as dt
from
	c_inbound
union all
SELECT
	c_outbound.upn as upn,
	c_outbound.outbound_qty as qty,
	c_outbound.sap_upl_level4_name,
	c_outbound.sap_upl_level5_name,
	c_outbound.division_display_name,
	c_outbound.standard_cost,
	c_outbound.delivery_plant,
	c_outbound.buffer_flag,
	c_outbound.distribution_properties as distribution_properties,
	c_outbound.mfn_tax_rate as mfn_tax_rate,
	c_outbound.hs_code as hs_code,
	c_outbound.unit_price as unit_price,
	c_outbound.provisional_tax_rate as provisional_tax_rate,
	c_outbound.origin_country as origin_country,
	c_outbound.ex_rate,
    'outbound' as flag_in_out,
	'2021-12-24' as dt
from
	c_outbound 
	
	
    ;
"
# 2. 执行加载数据SQL
$hive -e "$sql_str"

echo "End syncing data into DWS layer on $sync_year : .................."