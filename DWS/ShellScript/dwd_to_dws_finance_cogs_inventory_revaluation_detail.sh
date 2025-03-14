#!/bin/bash
# Function:
#   sync up dws_finance_cogs_inventory_revaluation_detail 
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

echo "start syncing dws_finance_cogs_inventory_revaluation_detail data into DWS layer on ${sync_date} : ${sync_date[year]}"

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

drop table if exists tmp_dws_finance_cogs_inventory_revaluation_detail;
create table  tmp_dws_finance_cogs_inventory_revaluation_detail stored as orc as 
	SELECT 
		xx.disvision
		,xx.year_mon
		,xx.value_all
		,xx.currency
		,ee.qty
		,xx.value_all/ee.qty as unit_price
	from (
	SELECT 
		disvision,
		years||mon_n as year_mon,
		sum(value_all) as value_all,
		currency
	from dwt_finance_monthly_cs_freight_ocgs_htm
	where items ='Prod_COGSAdj - Product COGS Adjustments'
		and  years||mon_n<=versions
	group by disvision,years||mon_n,currency
	)xx
	left join (	
		select 
			disvision,
			year_mon,
			sum(qty) qty
		from dws_dsr_billed_cr_qty_netvalue_detail
		group by disvision,year_mon
	)ee
	on xx.disvision=ee.disvision
	and xx.year_mon=ee.year_mon;

drop table if exists tmp_dws_finance_cogs_inventory_revaluation_detail_2;
create table  tmp_dws_finance_cogs_inventory_revaluation_detail_2 stored as orc as 
SELECT 
	a.disvision,
	a.material,
	a.qty,
	b.unit_price,
	a.qty*b.unit_price as inventory_revaluation_value,
	b.currency,
	a.year_mon
from  dws_dsr_billed_cr_qty_netvalue_detail a
left join tmp_dws_finance_cogs_inventory_revaluation_detail b
on a.disvision=b.disvision
and a.year_mon=b.year_mon;

insert overwrite table dws_finance_cogs_inventory_revaluation_detail partition(year_mon)
SELECT 
		disvision,
		material,
		qty,
		unit_price,
		inventory_revaluation_value,
		currency,
		year_mon
from tmp_dws_finance_cogs_inventory_revaluation_detail_2;

"
delete_tmp="
drop table tmp_dws_finance_cogs_inventory_revaluation_detail;
drop table tmp_dws_finance_cogs_inventory_revaluation_detail_2;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
$hive -e "$delete_tmp"
echo "End syncing dws_finance_cogs_inventory_revaluation_detail data into DWS layer on ${sync_date} .................."