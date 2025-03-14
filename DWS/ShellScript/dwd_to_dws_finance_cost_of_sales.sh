#!/bin/bash
# Function:
#   sync up dws_finance_cost_of_sales 
# History:
# 2023-10-23    slc   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi

echo "start syncing dws_finance_cost_of_sales data into DWS layer on ${sync_date} : ${sync_date[year]}"

# billed:
# dned:

sto_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

drop table if exists tmp_dws_finance_cogs_std_cogs_at_standard_detail;
create table  tmp_dws_finance_cogs_std_cogs_at_standard_detail stored as orc as 
select 
	a.disvision,
	a.material,
	a.value_all, 
	a.qty,
	cast(b.standard_cost_usd as decimal(18,2)) as standard_cost_usd,
	cast(b.standard_cost as decimal(18,2)) as standard_cost,
	a.year_mon
from dws_dsr_billed_cr_qty_netvalue_detail  a
left join dws_dim_material_by_mon b
on a.material=b.material_code
and a.year_mon=replace(b.months,'-','');

insert overwrite table dws_finance_cogs_std_cogs_at_standard_detail partition(year_mon)
SELECT 
		disvision,
		material,
		value_all AS value_cny,
		qty,
		standard_cost_usd,
		standard_cost,
		qty*coalesce(standard_cost_usd,0) as standard_cost_value,
		'USD'currency,
		year_mon
from tmp_dws_finance_cogs_std_cogs_at_standard_detail;


drop table if exists tmp_dws_finance_cogs_std_cogs_at_standard_gap;
create table  tmp_dws_finance_cogs_std_cogs_at_standard_gap stored as orc as 
select 
	xx.disvision	
	,xx.value_all
	,ee.value_all as standard_cost_value
	,xx.value_all-COALESCE(ee.value_all,0) as gap_value
	,xx.currency
	,xx.year_mon
from (
	SELECT 
		disvision,
		years||mon_n as year_mon,
		sum(value_all) as value_all,
		currency
	from dwt_finance_monthly_cs_freight_ocgs_htm
	where items ='COGS_Std - COGS at Standard'
			and  years||mon_n<=versions
	group by disvision,years||mon_n,currency)xx
left join (
	select disvision,year_mon,sum(standard_cost_value) as value_all
		from dws_finance_cogs_std_cogs_at_standard_detail
		group by disvision, year_mon)ee
on xx.disvision=ee.disvision
and xx.year_mon=ee.year_mon;

insert overwrite table dws_finance_cogs_std_cogs_at_standard_gap partition(year_mon)
SELECT 
		disvision,
		value_all,
		standard_cost_value,
		gap_value,
		currency,
		year_mon
from tmp_dws_finance_cogs_std_cogs_at_standard_gap;

"
delete_tmp="
drop table tmp_dws_finance_cogs_std_cogs_at_standard_gap;
drop table tmp_dws_finance_cogs_std_cogs_at_standard_detail;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dws_finance_net_sales_detail data into DWS layer on ${sync_date} .................."