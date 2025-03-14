#!/bin/bash
# Function:
#   sync up dwt_kpi_by_bu_detail 
# History:
# 2023-10-23    slc   v1.0    init
export LANG="en_US.UTF-8"
# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$2" ] ;then 
    sync_date=$2
else
    sync_date=$(date  +%F)
fi

echo "start syncing dwt_kpi_by_bu_detail data into DWS layer on ${sync_date} : ${sync_date[year]}"
max_dt_fenbo=`hdfs dfs -ls '/bsc/opsdw/dwd/dwd_outbound_distribution' | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_entry_sh_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_entry_sh | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_entry_tj_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_entry_tj | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_entry_cd_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_entry_cd | tail -n 1 | awk -F'=' '{print $NF}'`
#进口
ods_aws_inbound_iekpi_txn_pacemaker_sh_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_pacemaker_sh | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_custom_clear_sh_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_custom_clear_sh | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_custom_clear_malaysia_sh_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_custom_clear_malaysia_sh | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_custom_clear_tj_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_custom_clear_tj | tail -n 1 | awk -F'=' '{print $NF}'`
ods_aws_inbound_iekpi_txn_custom_clear_cd_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_aws_inbound_iekpi_txn_custom_clear_cd | tail -n 1 | awk -F'=' '{print $NF}'`
MAX_VERSION=`hdfs dfs -ls /bsc/opsdw/dws/dws_finance_monthly_cs_freight_ocgs_htm/sheet_name=OCOGS | tail -n 1 | awk -F'=' '{print $NF}'`
# billed:
# dned:

dws_ie_kpi_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
----第一次刷全年后续每次刷3个月数据
	select 
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		--case when kpicode='IE002' then 'DockWarrantDate-> Arrived Warehouse​​'
		--	 when kpicode='IE001' then 'T1 Pickup -> DockWarrantDate​'
		--	 when kpicode='WH014' then 'Arrive Warehouse -> MIGO'
		--end  as level4,		
		plant,
		dt,
		---coalesce(division_display_name,'others') as bu,
		null as bu,
		lt_day_cd value1, ---自然日
		lt_day_wd value2,	---工作日
		qty as flag1, 
		ee as flag2, 
		housewaybillno as flag3,
		'ET0101'||kpicode as kpi_code,
		year_mon
	from (	select 
		kpicode,
		plant,
		dt,
		sum(qty)qty,
		housewaybillno,
		count(1)ee,
		sum(case when kpicode='WH014' then FLOOR(lt_day_cd )/24 else FLOOR(lt_day_cd ) end )lt_day_cd,
		sum(case when kpicode='WH014' then FLOOR(lt_day_wd )/24 else FLOOR(lt_day_wd ) end )lt_day_wd,
		year_mon
	from dws_ie_kpi
	where year_mon>=substr(trunc('$sync_date','Q'),1,7) 
	and kpicode in ( 'IE001','IE002','WH014')
	and lt_day_cd is not null
	and lt_day_cd<365 
	--and need_calcaulated='1'
	group by kpicode,plant,dt,housewaybillno,year_mon)xx
	"
dws_kpi_stock_putaway_time_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'MIGO -> Putaway' as level4,		
		plant,
		dt,
		null as bu,
		processtime_cd value1, ---自然日
		process_wd_m value2,	---工作日
		qty as flag1, 
		ee as flag2, 
		delivery_no as flag3,
		'ET0101'||kpicode||'.1' as kpi_code,
		substr(dt,1,7) as year_mon
	from (select 
		kpicode,
		plant,
		dt,
		sum(qty)qty,
		delivery_no,
		count(1)ee,
		sum(FLOOR(processtime_cd)/24)processtime_cd,
		sum(FLOOR(process_wd_m)/24)process_wd_m
	from dws_kpi_stock_putaway_time
	where dt>=trunc('$sync_date','Q')and kpicode ='WH015' and processtime_cd is not null
	AND plant='D838'and processtime_cd<365
	group by kpicode,plant,dt,delivery_no)ee;
	"
dws_kpi_sto_migo2pgi_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'MIGO-> STO PGI' as level4,		
		plant,
		dt,
		null as bu,
		lt_cd_hr value1, ---自然日
		lt_dw_hr value2,	---工作日
		qty as flag1, 
		ee as flag2, 
		outbounddn as flag3,
		'ET0101'||kpi_no as kpi_code,
		substr(dt,1,7) as year_mon
	from (select 
		kpi_no,
		plant,
		dt,
		sum(qty)qty,
		outbounddn,
		count(1)ee,
		sum((lt_cd_hr))lt_cd_hr,
		sum((lt_dw_hr))lt_dw_hr
	from dws_kpi_sto_migo2pgi
	where dt>=trunc('$sync_date','Q') and kpi_no ='WH016' and lt_cd_hr is not null
	and lt_cd_hr/24<365
	group by kpi_no,plant,dt,outbounddn)rr
	;"
dws_kpi_zc_timi_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'STO PGI -> MIGO (STO Transportation)' as level4,		
		ship_from_plant as plant,
		dt,
		null as bu,  
		lt_cd_hr value1, ---自然日
		lt_dw_hr value2,	---工作日
		qty as flag1, 
		ee as flag2, 
		delivery_no as flag3,
		'ET0101'||kpi_no as kpi_code,
		substr(dt,1,7) as year_mon
	from (select 
		kpi_no,
		ship_from_plant,
		dt,
		sum(qty)qty,
		delivery_no,
		count(1)ee,
		sum(FLOOR(lt_cd_hr)/24)lt_cd_hr,
		sum(FLOOR(lt_dw_hr)/24)lt_dw_hr
	from dws_kpi_zc_timi
	where dt>=trunc('$sync_date','Q') and kpi_no ='WH018.1' and lt_cd_hr is not null
	and lt_cd_hr/24<365
	group by kpi_no,ship_from_plant,dt,delivery_no)qq

"
finance_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
-----------------财务指标Inventory Charges =C_004,  加C016只是后面辅助计算要用和inventory charges没关系
select	
	plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
	sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||case when itemcode='C_003' then 'C_016' else itemcode end as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_cogs_sharing_detail
where itemcode IN ('C_004','C_016','C_003')  and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and (plan_or_act='act' or (plan_or_act='plan' and category='PLNRST-AOP'))
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	plan_or_act,
	division_finace,
	case when itemcode='C_003' then 'C_016' else itemcode end
union all
select	
	'act' as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(value_excel) as value1,
	null as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||'C_016' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_net_sales_cogs_gap
where itemcode ='C_003'  and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	division_finace
;
-----------------财务指标net_sales,  计算的中间值不需要显示
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select	
	'act' as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(value_excel) as value1,
	null as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'SC0100' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_net_sales_cogs_gap
where itemcode ='S_001'  and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	division_finace,
	itemcode
union all
select	
	plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	null as value1,
	sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'SC0100' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_cogs_sharing_detail
where itemcode ='S_001'  and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and  (plan_or_act='plan' and category='PLNRST-AOP')
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	plan_or_act,
	division_finace,
	itemcode;
-----------------财务指标cosgs,  计算的中间值不需要显示	
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select	
	plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
	sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||'cogs' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_cogs_sharing_detail
where itemcode IN (
'C_001'
,'C_002'
,'C_003'
,'C_005'
,'C_008'
,'C_009'
,'C_010'
,'C_011'
,'C_012'
,'C_013'
,'C_015'
,'C_016'
,'C_017')  and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and (plan_or_act='act' or (plan_or_act='plan' and category='PLNRST-AOP'))
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	plan_or_act,
	division_finace,
	itemcode
UNION ALL 
select	
	'act' as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(value_excel) as value1,
	null as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||'cogs' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_net_sales_cogs_gap
where itemcode IN ('C_001','C_003')  and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	division_finace,
	itemcode
union all
select	
	plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
	sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||'cogs' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_cogs_sharing_detail
where   year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and ((plan_or_act='act' and itemcode='C_014.1' )or (itemcode='C_014' and plan_or_act='plan' and category='PLNRST-AOP'))
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	plan_or_act,
	division_finace,
	itemcode
UNION ALL 
select	
	'act' as plant,
	from_unixtime(unix_timestamp(months||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_excel as bu,
	sum(finall_amount) as value1,
	null as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||'cogs' as kpi_code,
	from_unixtime(unix_timestamp(months||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_eeo
where    months>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
--itemcode IN ('C_006','C_007','C_008')
and months<='$MAX_VERSION'
group by 	months,
	division_excel,
	itemcode;
-------财务指标Logistic Cost
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
--包含
--E_019，SCS
--E_010，Logistics Specialist
--E_018，Scrap cost
--E_017，Operation Fee
--E_009，Fesco Charge
--E_021，国科租仓费
--E_022，移库宝山运输费
--E_020，Outside Services
--E_026，Sample (商检样品)
--E_029，Small labels
--E_031，Office supplies + 快递费用
--E_030，Envelope +DN bag+Patient card（调整为DN Bag）
--E_027，Carton+Tape
--E_028，Filling materials
--E_032，塑料循环箱项目
--E_033，循环箱及测试费用
--E_036，Others
--E_034，Insurance (domestic freight)
--E_035，Freight (BSCL) ---opex表的
--E_038.1，Facilities
--E_039，Depreciation
--E_041 Freight (BSCL)  --FN表的freght
select 
	plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	coalesce(division_finace,'Others') as bu,
	sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
	sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0200' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') year_mon
from dws_finance_cogs_sharing_detail
where itemcode in (	
	'E_019'
    ,'E_010'
    ,'E_018'
    ,'E_017'
    ,'E_009'
    ,'E_021'
    ,'E_022'
    ,'E_020'
    ,'E_026'
    ,'E_029'
    ,'E_031'
    ,'E_030'
    ,'E_027'
    ,'E_028'
    ,'E_032'
    ,'E_033'
    ,'E_036'
    ,'E_034'
    ,'E_035'
    ,'E_038.1'
	,'E_038'
    ,'E_039'
	,'E_041')
and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and (plan_or_act='act' or (plan_or_act='plan'))
and value_sharing is not null
and year_mon<='$MAX_VERSION'
and case when itemcode='E_038' and plan_or_act='act' then 1 else 0 end =0
group by 	year_mon,
	plan_or_act,
	coalesce(division_finace,'Others');
---------------------------------------------IE agency Cost 
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select 
		plan_or_act as plant,
		from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
		division_finace as bu,
		sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
		sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
		null as flag1, 
		null as flag2, 
		null as flag3,
		'TS0300' as kpi_code,
		from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') year_mon
	from dws_finance_cogs_sharing_detail
	where itemcode in (	'E_013','E_014','E_015','E_011','E_012')
	and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
	and (plan_or_act='act' or (plan_or_act='plan' and category='PLNRST-AOP'))
	and year_mon<='$MAX_VERSION'
	and value_sharing is not null
	group by 	year_mon,
		plan_or_act,
		division_finace;
-------财务指标OPEX Others Cost
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
	sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0500' as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') year_mon
from dws_finance_cogs_sharing_detail
where itemcode in (	
'E_001'
,'E_002'
,'E_003'
,'E_004'
,'E_005'
,'E_006'
,'E_007'
,'E_008'
,'E_016'
,'E_023'
,'E_024'
,'E_025'
,'E_040')
and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
and (plan_or_act='act' or (plan_or_act='plan' ))
and value_sharing is not null
and year_mon<='$MAX_VERSION'
group by 	year_mon,
	plan_or_act,
	division_finace;
---------------------------------------------Duty Cost
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select 
		coalesce(category,plan_or_act) as plant,
		from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
		division_finace as bu,
		sum(case when plan_or_act='act' then  value_sharing else 0 end) as value1,
		sum(case when plan_or_act='plan' then  value_sharing else 0 end) as value2,
		null as flag1, 
		null as flag2, 
		null as flag3,
		'TS0400' as kpi_code,
		from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') year_mon
	from dws_finance_cogs_sharing_detail
	where( (itemcode ='C_014' and plan_or_act='plan'and category ='PLNRST-AOP' )or (itemcode ='C_014.1' and  plan_or_act='act'))
	and year_mon>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
	and year_mon<='$MAX_VERSION'
	and value_sharing is not null
	group by 	year_mon,
		coalesce(category,plan_or_act),
		division_finace;
---------------------------------------------OCOGS Others Cost
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select 
		plan_or_act as plant,
		from_unixtime(unix_timestamp(years||mon_n||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
		division_finace as bu,
		sum(case when plan_or_act='act' then  value_all_cny else 0 end) as value1,
		sum(case when plan_or_act='plan' then  value_all_cny else 0 end) as value2,
		null as flag1, 
		null as flag2, 
		null as flag3,
		'TS0600' as kpi_code,
		years||'-'||mon_n year_mon
	from dwt_finance_monthly_cs_freight_ocgs_htm
	where items ='COGS - COGS (incl GAAP Adj)' 
	and value_all_cny>0 
	and years||mon_n>=replace(substr(add_months(trunc('$sync_date','MM'),-3),1,7),'-','') 
	and years||mon_n<='$MAX_VERSION'
	group by 	years||mon_n,
	years||'-'||mon_n,
		plan_or_act,
		division_finace;

"
# union all
# select 
# 'act' as plant,
# DocDate as dt,

# from dwd_fact_trans_fs10ndetail	
# where yearmonth>='2023-01'
# and trim(account)='500019'
duty_by_upn_histest_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
---select 
---	sites as plant,
---	from_unixtime(unix_timestamp(year_mon||'-01', 'yyyy-MM-dd'), 'yyyy-MM-dd') as dt,	
---	coalesce(b.excel,REGEXP_REPLACE(a.division,'[^a-zA-Z]','') )as bu,
---	sum(sharing_tariffs_benchmark_total_price) as value1,--分摊关税基准总价
---	null as value2,
---	null as flag1, 
---	null as flag2, 
---	null as flag3,
---	'TS0400' as kpi_code,
---	year_mon
---from dwd_duty_by_upn_histest a
---left join dwd_dim_finance_disvision_mapping_hive_disvsion b 
---	on REGEXP_REPLACE(a.division,'[^a-zA-Z]','')=b.hive
---where  year_mon>='2023-01'
---group by 	year_mon,
---	sites,
---	coalesce(b.excel,REGEXP_REPLACE(a.division,'[^a-zA-Z]','') )
---union all

"
clearance_leadtime_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';
--------------------------------------------------------------------------收到清单时间->清关完成时间(分拨)​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select
		--收到清单时间->清关完成时间(分拨)
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'STO PGI -> MIGO (STO Transportation)' as level4,		
		'D835' as plant,
		substr(customclearance_date,1,10) as dt,  --清关完成时间(分拨)​
		null as bu, 
		distirib_custom_cd value1, ---自然日
		distirib_custom_wd value2,	---工作日
		 '非起搏器' as flag1, 
		ee as flag2, 
		biz_no as flag3, 
		'ET0102'||'IE006.1' as kpi_code,
		substr(customclearance_date,1,7) as year_mon
	from (select 
			substring(customclearance_date,1,10) as customclearance_date,
			biz_no,
			count(1)ee,
			sum(distirib_custom_cd)distirib_custom_cd, ---自然日
			sum(distirib_custom_wd)distirib_custom_wd	---工作日
		from dwd_outbound_distribution
		where 	dt='$max_dt_fenbo' 
		and substring(customclearance_date,1,1)='2'
		and substring(distributedoc_receivedate,1,1)='2' --收到清单时间
		group by substring(customclearance_date,1,10),biz_no)xx --sh部分
;
--------------------------------------------------------------------------舱单确认->入库
drop table dwt_kpi_by_bu_detail_dock_invent;
create table dwt_kpi_by_bu_detail_dock_invent as 
SELECT businessnumber, warehousereceiptconfirmationtimeemail as dockwarrantdate,warehousingtime,year ,'D835'AS plant
from ods_aws_inbound_iekpi_txn_entry_sh
where year ='$ods_aws_inbound_iekpi_txn_entry_sh_maxdt' 
union all
SELECT businessnumber,customsmanifesttime as dockwarrantdate,warehousingtime,year,'D837'AS plant
from ods_aws_inbound_iekpi_txn_entry_tj
where year ='$ods_aws_inbound_iekpi_txn_entry_tj_maxdt'
union all
SELECT 
recordnumber as businessnumber,customsmanifesttime as dockwarrantdate,warehousingtime,year,'D836'AS plant
from ods_aws_inbound_iekpi_txn_entry_cd
where year ='$ods_aws_inbound_iekpi_txn_entry_cd_maxdt'
;

insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	intoinventorydate as dt,  --入库时间
	null as bu,  
	sum(dock_invent_cd)value1, ---自然日
	sum(dock_invent_wd)value2,	---工作日
	'非起搏器' as flag1, --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE006' as kpi_code,
	substr(intoinventorydate,1,7) as year_mon
from (select 
		plant,
		substring(warehousingtime,1,10) as intoinventorydate,
		businessnumber, ---业务编号
		CAST((unix_timestamp(warehousingtime) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)/24  as dock_invent_cd, 
		(CAST((unix_timestamp(warehousingtime) - unix_timestamp(dockwarrantdate))/(60 * 60) AS float)
					- myudf(dockwarrantdate,warehousingtime)*24 )/24 as dock_invent_wd 
	from  dwt_kpi_by_bu_detail_dock_invent 
	where businessnumber is not null 
		and substring(warehousingtime,1,7) >='2023-01'
		and substring(dockwarrantdate,1,1)='2'
		and substring(warehousingtime,1,1)='2'
)aa
group by plant
	,intoinventorydate
	,businessnumber
	,substr(intoinventorydate,1,7)
;

--------------------------------------------------------------------------入库->出库、二放(TJ，CD,SH),起搏器
drop table dwt_kpi_by_bu_detail_invent_outbound;
create table dwt_kpi_by_bu_detail_invent_outbound as 
SELECT 
	businessnumber, 
	warehousingtime as intoinventorydate, --入库
	taxapplicationtime, --付税申请时间
	taxcompletiontime as customrelease_1, --付税完成当作海关一放
	localizationcompletiontime as chineselabelpicturereceiveddate, --本地化作为中文标签
	commodityinspectiontime as commodityinspection_date, ---商检查验时间
	actualinspectiontime as actualtest_date, ---实际检测时间
	ciqsignaturecompletion as outbound_dt, --签字完成
	year ,
	'起搏器' as catorgry,
	'D835'AS plant
from ods_aws_inbound_iekpi_txn_pacemaker_sh  ---起搏器
where year ='$ods_aws_inbound_iekpi_txn_pacemaker_sh_maxdt' 
union all
SELECT 
	businessnumber,
	warehousingtime as intoinventorydate, --入库
	taxapplicationtime, --付税申请时间
	taxcompletiontime as customrelease_1, --付税完成当作海关一放
	chineselabelphotoreceivedtime as chineselabelpicturereceiveddate, --中文标签
	commodityinspectiontime as commodityinspection_date,--商检查验
	null as actualtest_date, ---实际检测时间
	secondrelease as outbound_dt, ---海关二放
	year,
	'非起搏器' as catorgry,
	'D835'AS plant
from ods_aws_inbound_iekpi_txn_custom_clear_sh  --上海出库清关
where year ='$ods_aws_inbound_iekpi_txn_custom_clear_sh_maxdt'
union all
SELECT 
	businessnumber,
	warehousingtime as intoinventorydate, --入库
	taxapplicationtime, --付税申请时间
	taxcompletiontime as customrelease_1, --付税完成当作海关一放
	chineselabelphotoreceivedtime as chineselabelpicturereceiveddate, --中文标签
	commodityinspectiontime as commodityinspection_date,--商检查验
	null as actualtest_date, ---实际检测时间
	secondrelease as outbound_dt, ---海关二放
	year,
	'非起搏器' as catorgry,
	'D835'AS plant
from ods_aws_inbound_iekpi_txn_custom_clear_malaysia_sh  --上海出库清关马来西亚
where year ='$ods_aws_inbound_iekpi_txn_custom_clear_malaysia_sh_maxdt'
union all
SELECT 
	businessnumber,
	warehousingtime as intoinventorydate, --入库
	taxapplicationtime,--付税申请时间
	taxcompletiontime as customrelease_1, --付税完成当作海关一放
	null as chineselabelpicturereceiveddate, --中文标签
	null as commodityinspection_date,--商检查验,非法检的用海关一放
	null as actualtest_date, ---实际检测时间
	coalesce(secondreleasetime ,
			case when commercialinspectiontime3 like '%非法检%' 
				then customsclearancecompletiontime 
				else commercialinspectiontime3 
			end)as outbound_dt, ---海关二放，没有就用商检查验，非法检的用海关一放
	year,
	'非起搏器' as catorgry,
	'D837'AS plant
from ods_aws_inbound_iekpi_txn_custom_clear_tj  --天津出库清关
where year ='$ods_aws_inbound_iekpi_txn_custom_clear_tj_maxdt'
union all
SELECT 
	serialnumber as businessnumber,
	warehousingtime as intoinventorydate, --入库
	taxapplicationtime,--付税申请时间
	taxcompletiontime  as customrelease_1, --付税完成当作海关一放
	null as chineselabelpicturereceiveddate, --中文标签
	null as commodityinspection_date,--商检查验
	null as actualtest_date, ---实际检测时间
	commodityinspectiontime as outbound_dt, ---商检查验
	year,
	'非起搏器' as catorgry,
	'D836'AS plant
from ods_aws_inbound_iekpi_txn_custom_clear_cd  --成都出库清关
where year ='$ods_aws_inbound_iekpi_txn_custom_clear_cd_maxdt'
;
--------------------------------------------------------------------------入库时间->付税申请​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	taxapplicationtime as dt,  --付税申请
	null as bu,  
	sum(invent_cust1_cd)value1, ---自然日
	sum(invent_cust1_wd)value2,	---工作日
	catorgry as flag1, --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET010201' as kpi_code,
	substr(taxapplicationtime,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(taxapplicationtime,1,10) as taxapplicationtime,
		businessnumber, ---业务编号
		CAST((unix_timestamp(taxapplicationtime) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)/24  as invent_cust1_cd, 
		(CAST((unix_timestamp(taxapplicationtime) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
					- myudf(intoinventorydate,taxapplicationtime)*24 )/24 as invent_cust1_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(taxapplicationtime,1,7) >='2023-01'
		and substring(intoinventorydate,1,1)='2'
		and substring(taxapplicationtime,1,1)='2'
)aa
group by plant
	,catorgry
	,taxapplicationtime
	,businessnumber
	,substr(taxapplicationtime,1,7)
;
--------------------------------------------------------------------------中文标签->二放​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	outbound_dt as dt,  --二放​
	null as bu,  
	sum(invent_cust1_cd)value1, ---自然日
	sum(invent_cust1_wd)value2,	---工作日
	catorgry as flag1, --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET010202' as kpi_code,
	substr(outbound_dt,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(outbound_dt,1,10) as outbound_dt,
		businessnumber, ---业务编号
		CAST((unix_timestamp(outbound_dt) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)/24  as invent_cust1_cd, 
		(CAST((unix_timestamp(outbound_dt) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)
					- myudf(chineselabelpicturereceiveddate,outbound_dt)*24 )/24 as invent_cust1_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(outbound_dt,1,7) >='2023-01'
		and substring(chineselabelpicturereceiveddate,1,1)='2'
		and substring(outbound_dt,1,1)='2'
)aa
group by plant
	,catorgry
	,outbound_dt
	,businessnumber
	,substr(outbound_dt,1,7)
;
--------------------------------------------------------------------------入库->出库、二放​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	outbound_dt as dt,  --二放时间
	null as bu,  --是否起搏器
	sum(invent_out_cd)value1, ---自然日
	sum(invent_out_wd)value2,	---工作日
	catorgry as flag1, --是否分拨
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE018' as kpi_code,
	substr(outbound_dt,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(outbound_dt,1,10) as outbound_dt,
		businessnumber, ---业务编号
		CAST((unix_timestamp(outbound_dt) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)/24  as invent_out_cd, 
		(CAST((unix_timestamp(outbound_dt) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
					- myudf(intoinventorydate,outbound_dt)*24 )/24 as invent_out_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(outbound_dt,1,7) >='2023-01'
		and substring(intoinventorydate,1,1)='2'
		and substring(outbound_dt,1,1)='2'
)aa
group by plant
	,catorgry
	,outbound_dt
	,businessnumber
	,substr(outbound_dt,1,7)
;

--------------------------------------------------------------------------入库时间->海关一放时间​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	customrelease_1 as dt,  --海关一放时间​
	null as bu,  --是否起搏器
	sum(invent_cust1_cd)value1, ---自然日
	sum(invent_cust1_wd)value2,	---工作日
	catorgry as flag1, --是否分拨
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE007' as kpi_code,
	substr(customrelease_1,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(customrelease_1,1,10) as customrelease_1,
		businessnumber, ---业务编号
		CAST((unix_timestamp(customrelease_1) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)/24  as invent_cust1_cd, 
		(CAST((unix_timestamp(customrelease_1) - unix_timestamp(intoinventorydate))/(60 * 60) AS float)
					- myudf(intoinventorydate,customrelease_1)*24 )/24 as invent_cust1_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(customrelease_1,1,7) >='2023-01'
		and substring(intoinventorydate,1,1)='2'
		and substring(customrelease_1,1,1)='2'
)aa
group by plant
	,catorgry
	,customrelease_1
	,businessnumber
	,substr(customrelease_1,1,7)
;
--------------------------------------------------------------------------海关一放时间->收到仓库中文标签照片时间,​只有上海有
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	chineselabelpicturereceiveddate as dt,  --海关一放时间​
	null as bu,  
	sum(cust1_picture_cd)value1, ---自然日
	sum(cust1_picture_wd)value2,	---工作日
	catorgry as flag1,  --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE008' as kpi_code,
	substr(chineselabelpicturereceiveddate,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(chineselabelpicturereceiveddate,1,10) as chineselabelpicturereceiveddate,
		businessnumber, ---业务编号
		CAST((unix_timestamp(chineselabelpicturereceiveddate) - unix_timestamp(customrelease_1))/(60 * 60) AS float)/24  as cust1_picture_cd, 
		(CAST((unix_timestamp(chineselabelpicturereceiveddate) - unix_timestamp(customrelease_1))/(60 * 60) AS float)
					- myudf(customrelease_1,chineselabelpicturereceiveddate)*24 )/24 as cust1_picture_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(chineselabelpicturereceiveddate,1,7) >='2023-01'
		and substring(customrelease_1,1,1)='2'
		and substring(chineselabelpicturereceiveddate,1,1)='2'
)aa
group by plant
	,catorgry
	,chineselabelpicturereceiveddate
	,businessnumber
	,substr(chineselabelpicturereceiveddate,1,7)
;
--------------------------------------------------------------------------收到仓库中文标签照片时间->商检查验时间​​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	commodityinspection_date as dt,  --海关一放时间​
	null as bu, 
	sum(picture_comm_cd)value1, ---自然日
	sum(picture_comm_wd)value2,	---工作日
	catorgry as flag1,  --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE009' as kpi_code,
	substr(commodityinspection_date,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(commodityinspection_date,1,10) as commodityinspection_date,
		businessnumber, ---业务编号
		CAST((unix_timestamp(commodityinspection_date) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)/24  as picture_comm_cd, 
		(CAST((unix_timestamp(commodityinspection_date) - unix_timestamp(chineselabelpicturereceiveddate))/(60 * 60) AS float)
					- myudf(chineselabelpicturereceiveddate,commodityinspection_date)*24 )/24 as picture_comm_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(commodityinspection_date,1,7) >='2023-01'
		and substring(chineselabelpicturereceiveddate,1,1)='2'
		and substring(commodityinspection_date,1,1)='2'
)aa
group by plant
	,catorgry
	,commodityinspection_date
	,businessnumber
	,substr(commodityinspection_date,1,7)
;

--------------------------------------------------------------------------商检查验时间->实际检测时间(起搏器)
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	actualtest_date as dt,  --实际检测
	null as bu,  
	sum(comm_act_cd)value1, ---自然日
	sum(comm_act_wd)value2,	---工作日
	catorgry as flag1, --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE010' as kpi_code,
	substr(actualtest_date,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(actualtest_date,1,10) as actualtest_date,
		businessnumber, ---业务编号
		CAST((unix_timestamp(actualtest_date) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)/24  as comm_act_cd, 
		(CAST((unix_timestamp(actualtest_date) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)
					- myudf(commodityinspection_date,actualtest_date)*24 )/24 as comm_act_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(actualtest_date,1,7) >='2023-01'
		and substring(commodityinspection_date,1,1)='2'
		and substring(actualtest_date,1,1)='2'
)aa
group by plant
	,catorgry
	,actualtest_date
	,businessnumber
	,substr(actualtest_date,1,7)
;

--------------------------------------------------------------------------实际检测时间->两证完成(起搏器)
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	outbound_dt as dt,  --两证完成
	null as bu,  
	sum(act_out_cd)value1, ---自然日
	sum(act_out_wd)value2,	---工作日
	catorgry as flag1, --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE011' as kpi_code,
	substr(outbound_dt,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(outbound_dt,1,10) as outbound_dt,
		businessnumber, ---业务编号
		CAST((unix_timestamp(outbound_dt) - unix_timestamp(actualtest_date))/(60 * 60) AS float)/24  as act_out_cd, 
		(CAST((unix_timestamp(outbound_dt) - unix_timestamp(actualtest_date))/(60 * 60) AS float)
					- myudf(actualtest_date,outbound_dt)*24 )/24 as act_out_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(outbound_dt,1,7) >='2023-01'
		and substring(actualtest_date,1,1)='2'
		and substring(outbound_dt,1,1)='2'
)aa
group by plant
	,catorgry
	,outbound_dt
	,businessnumber
	,substr(outbound_dt,1,7)
;

--------------------------------------------------------------------------商检查验时间->海关二放时间(非起搏器)
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	outbound_dt as dt,  --实际检测
	null as bu,  
	sum(comm_act_cd)value1, ---自然日
	sum(comm_act_wd)value2,	---工作日
	catorgry as flag1, --是否起搏器
	count(1) as flag2,
	businessnumber as flag3, 
	'ET0102IE012' as kpi_code,
	substr(outbound_dt,1,7) as year_mon
from (select 
		plant,
		catorgry,
		substring(outbound_dt,1,10) as outbound_dt,
		businessnumber, ---业务编号
		CAST((unix_timestamp(outbound_dt) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)/24  as comm_act_cd, 
		(CAST((unix_timestamp(outbound_dt) - unix_timestamp(commodityinspection_date))/(60 * 60) AS float)
					- myudf(commodityinspection_date,outbound_dt)*24 )/24 as comm_act_wd 
	from  dwt_kpi_by_bu_detail_invent_outbound 
	where businessnumber is not null 
		and substring(outbound_dt,1,7) >='2023-01'
		and substring(commodityinspection_date,1,1)='2'
		and substring(outbound_dt,1,1)='2'
)aa
group by plant
	,catorgry
	,outbound_dt
	,businessnumber
	,substr(outbound_dt,1,7)
;
"
warehouse_dn_lt_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';
--------------------------------------------------------------------------Warehouse DN lead time
drop table if exists tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt;
create table tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt stored as orc as
SELECT 
	outbounddelivery,inboundasn,invoice,material,batch,outboundtype,operatetime,operatetime_max as pgi_time,quantity,nu,
	lead(nu)over(partition by outbounddelivery,inboundasn,invoice,material,batch order by nu) as sto_flag,
	lead(operatetime_max)over(partition by outbounddelivery,inboundasn,invoice,material,batch order by nu) as check_time
from (
select outbounddelivery,inboundasn,invoice,material,batch,outboundtype,min(operatetime)operatetime,max(operatetime)operatetime_max,sum(quantity)quantity,
case when outboundtype='创建SO' then 4
	 when outboundtype='STO拣货复核' then 2
	 when outboundtype='非STO装箱复核' then 3
	 when outboundtype='PGI' then 1
else 5 end as nu
from opsdw.dwd_fact_trans_wmsoutboundinfo
where dt>=trunc('$sync_date','Q') and outboundtype in ('PGI','创建SO','非STO装箱复核','STO拣货复核')
group by outbounddelivery,inboundasn,invoice,material,batch,outboundtype
order by outbounddelivery,inboundasn,invoice,material,batch,case when outboundtype='创建SO' then 4
	 when outboundtype='STO拣货复核' then 2
	 when outboundtype='非STO装箱复核' then 3
	 when outboundtype='PGI' then 1
else 5 end
)xx;

drop table if exists tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt_2;
create table tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt_2 stored as orc as
select 
	xx.outbounddelivery,
	xx.inboundasn,
	xx.invoice,
	xx.material,
	xx.batch,
	xx.outboundtype,
	xx.sto_flag,
	xx.pgi_time,
	xx.quantity,
	ee.division_display_name as bu,
	yy.so_createtime,
	 CAST((unix_timestamp(xx.pgi_time) - unix_timestamp(yy.so_createtime))/(60 * 60) AS float)/24  as soct_pgi_cd,
 (case when CAST((unix_timestamp(xx.pgi_time) - unix_timestamp(yy.so_createtime))/(60 * 60) AS float)
             - myudf(yy.so_createtime,xx.pgi_time)*24 <0 then 0
       else CAST((unix_timestamp(xx.pgi_time) - unix_timestamp(yy.so_createtime))/(60 * 60) AS float)
             - myudf(yy.so_createtime,xx.pgi_time)*24  
       end)/24 as soct_pgi_wd 
from (
select outbounddelivery,inboundasn,invoice,material,batch,outboundtype,sto_flag,pgi_time,quantity
from tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt
where nu=1)xx
left join (
	select outbounddelivery,material,min(operatetime)so_createtime
	from tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt
	where nu=4
	group by outbounddelivery,material)yy
	on xx.outbounddelivery=yy.outbounddelivery
	and xx.material=yy.material
left join (select material_code,division_display_name from dwd_dim_material where dt='$dwd_dim_material_maxdt')ee
	on xx.material=ee.material_code
;
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	null as plant,
	substr(pgi_time,1,10) as dt, 
	coalesce(bu,'Others'), 
	sum(soct_pgi_cd)value1, ---自然日
	sum(soct_pgi_wd)value2,	---工作日
	sto_flag as flag1, --2就是STO拣货复核,3就是非STO装箱复核
	sum(quantity) as flag2, 
	count(1) as flag3, 
	'OC0102' as kpi_code,
	substr(pgi_time,1,7) as year_mon
from tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt_2
group by substr(pgi_time,1,10),bu,sto_flag,substr(pgi_time,1,7)
union all
select 
	null as plant,
	substr(pgi_time,1,10) as dt, 
	'TOTAL CHINA'bu, 
	sum(soct_pgi_cd)value1, ---自然日
	sum(soct_pgi_wd)value2,	---工作日
	sto_flag as flag1, --2就是STO拣货复核,3就是非STO装箱复核
	sum(quantity) as flag2, 
	count(1) as flag3, 
	'OC0102' as kpi_code,
	substr(pgi_time,1,7) as year_mon
from tmp_dwt_kpi_by_bu_detail_warehouse_dn_lt_2
group by substr(pgi_time,1,10),sto_flag,substr(pgi_time,1,7)
;
"
STO_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------STO占比
drop table if exists tmp_dwt_kpi_by_bu_detail_sto;
create table tmp_dwt_kpi_by_bu_detail_sto stored as orc as
	select
		distinct 
		xx.inbounddelivery
	from (select 
	distinct
		delivery_no
	from dws_kpi_stock_putaway_time
	where dt>=trunc('$sync_date','Q') and kpicode ='WH015' 
	and processtime_cd is not null
	AND plant='D838')ee
	inner join (select outbounddelivery,inbounddelivery from opsdw.dwd_fact_trans_wmsoutboundinfo
		where dt>=add_months(trunc('$sync_date','Q'),-1) and outboundtype ='STO拣货复核')xx
	on ee.delivery_no=xx.outbounddelivery
;
drop table if exists tmp_dwt_kpi_by_bu_detail_sto2;
create table tmp_dwt_kpi_by_bu_detail_sto2 stored as orc as
	select	
		plant,
		dt,		
		--(processtime_cd/24)value1, ---自然日,放不下反正也默认是工作日来计算
		case when xx.inbounddelivery is not null then 1 else 0 end value1, ---计算外仓占比的
		(process_wd_m/24)value2,	---工作日
		qty as flag1, 
		ee as flag2, 
		delivery_no as flag3,
		'ET0101'||kpicode as kpi_code,
		substr(dt,1,7) as year_mon
	from (select 
		kpicode,
		plant,
		dt,
		delivery_no,
		sum(qty)qty,
		COUNT(1)ee,
		SUM(FLOOR(processtime_cd))processtime_cd,
		SUM(FLOOR(process_wd_m))process_wd_m
	from dws_kpi_stock_putaway_time
	where dt>=trunc('$sync_date','Q') and kpicode ='WH015' and processtime_cd is not null
	AND plant<>'D838'
	group by kpicode,plant,dt,delivery_no)ee
	LEFT join tmp_dwt_kpi_by_bu_detail_sto xx 
		on ee.delivery_no=xx.inbounddelivery
;
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	plant,
	dt, 
	null as bu,
	value1, ---自然日
	value2,	---工作日
	flag1, --转仓数量
	flag2, --数据量
	flag3, --delivery数量
	kpi_code,
	year_mon
from tmp_dwt_kpi_by_bu_detail_sto2;
"
WorkOrder_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------本地化数量摊分


"
Facilities_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------先库位摊分,再upn摊分


"



delete_Facilities_tmp="
drop table tmp_dws_finance_cogs_sharing_detail_Facilities;
drop table tmp_dws_finance_cogs_sharing_detail_Facilities_02;
drop table tmp_dws_finance_cogs_sharing_detail_Facilities_03;
"
# 2. 执行加载数据SQL
# 记录脚本开始执行的时间  
start_time=$(date +%s)  
#   
if [ "$1"x = "inbound_leadtime"x ];then
	echo "dws $1 only run"
	echo "$dws_ie_kpi_sql"
	$hive -e "$dws_ie_kpi_sql"
	echo "$dws_kpi_stock_putaway_time_sql"
	$hive -e "$dws_kpi_stock_putaway_time_sql"
	echo "$dws_kpi_sto_migo2pgi_sql"
	$hive -e "$dws_kpi_sto_migo2pgi_sql"
	echo "$dws_kpi_zc_timi_sql"
	$hive -e "$dws_kpi_zc_timi_sql"
	sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_kpi_by_bu_detail
	sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_kpi_by_bu_detail
elif [ "$1"x = "dws_ie_kpi_sql"x ];then
	echo "dws $1 only run"	
	echo "$dws_ie_kpi_sql"
	$hive -e "$dws_ie_kpi_sql"
	sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_kpi_by_bu_detail
	sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_kpi_by_bu_detail
elif [ "$1"x = "dws_kpi_stock_putaway_time_sql"x ];then
	echo "dws $1 only run"	
	echo "$dws_kpi_stock_putaway_time_sql"
	$hive -e "$dws_kpi_stock_putaway_time_sql"
elif [ "$1"x = "dws_kpi_sto_migo2pgi_sql"x ];then
	echo "dws $1 only run"	
	echo "$dws_kpi_sto_migo2pgi_sql"
	$hive -e "$dws_kpi_sto_migo2pgi_sql"
elif [ "$1"x = "dws_kpi_zc_timi_sql"x ];then
	echo "dws $1 only run"	
	echo "$dws_kpi_zc_timi_sql"
	$hive -e "$dws_kpi_zc_timi_sql"
elif [ "$1"x = "finance"x ];then
	echo "dws $1 only run"	
	echo "$finance_sql"
	$hive -e "$finance_sql"
	sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_kpi_by_bu_detail
	sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_kpi_by_bu_detail
elif [ "$1"x = "duty_by_upn_his"x ];then
	echo "dws $1 only run"	
	echo "$duty_by_upn_histest_sql"
	$hive -e "$duty_by_upn_histest_sql"
	#$hive -e "$delete_onhand_E_020_tmp"
elif [ "$1"x = "clearance_leadtime"x ];then
	echo "$clearance_leadtime_sql "	
	$hive -e "$clearance_leadtime_sql"
	sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_kpi_by_bu_detail
	sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_kpi_by_bu_detail
elif [ "$1"x = "warehouse_dn_lt"x ];then
	echo " $warehouse_dn_lt_sql"	
	$hive -e "$warehouse_dn_lt_sql"
elif [ "$1"x = "inbound_sto"x ];then
	echo " $STO_sql"	
	$hive -e "$STO_sql"
	sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_kpi_by_bu_detail
	sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_kpi_by_bu_detail
# elif [ "$1"x = "workorder"x ];then
# 	echo "dws $1 only run"	
# 	$hive -e "$WorkOrder_sql"
# elif [ "$1"x = "Facilities"x ];then
# 	echo "dws $1 only run"	
# 	$hive -e "$Facilities_sql"
else
    echo "--------------------------------------------------------------------------inbound_leadtime_sql"
    	echo "$dws_ie_kpi_sql"
	$hive -e "$dws_ie_kpi_sql"
	echo "$dws_kpi_stock_putaway_time_sql"
	$hive -e "$dws_kpi_stock_putaway_time_sql"
	echo "$dws_kpi_sto_migo2pgi_sql"
	$hive -e "$dws_kpi_sto_migo2pgi_sql"
	echo "$dws_kpi_zc_timi_sql"
	$hive -e "$dws_kpi_zc_timi_sql"
    echo "--------------------------------------------------------------------------finance sql"
    $hive -e "$finance_sql"
    echo "--------------------------------------------------------------------------duty_by_upn_histest_sql"
    $hive -e "$duty_by_upn_histest_sql"
    echo "--------------------------------------------------------------------------clearance_leadtime"
    $hive -e "$clearance_leadtime_sql"
	echo "--------------------------------------------------------------------------OC0102"
	$hive -e "$warehouse_dn_lt_sql"
	echo "--------------------------------------------------------------------------sto"
	$hive -e "$STO_sql"
	# echo "-----------------------------------------E_027,28,29----------------------workorder"
	# $hive -e "$WorkOrder_sql"
	# echo "-----------------------------------------E_038----------------------Facilities"
	# $hive -e "$Facilities_sql"
	# echo "--------------------------------------------------------------------------delete tmp table"
	# $hive -e "$delete_delivery_tmp"
	# $hive -e "$delete_inbound_tmp"
	# $hive -e "$delete_onhand_E_020_tmp"
	# $hive -e "$delete_sto_E_022_tmp"
	# $hive -e "$delete_SCRAP_COST_E_018_tmp"
	# $hive -e "$delete_workorder_tmp"	
	# $hive -e "$delete_PULIC_tmp"
	# $hive -e "$delete_Facilities_tmp"
	sh /bscflow/PG/Shell/all_dsr_to_hdfs.sh dwt_kpi_by_bu_detail
	sh /bscflow/PG/Shell/all_dsr_to_pg_db.sh dwt_kpi_by_bu_detail
fi
echo "End syncing dwt_kpi_by_bu_detail data into DWS layer on ${sync_date} .................."
# 记录脚本结束执行的时间  
end_time=$(date +%s)  
  
# 计算脚本运行时间（以秒为单位）  
execution_time=$((end_time - start_time))  
  
# 将秒转换为分钟和秒的形式  
minutes=$((execution_time / 60))  
seconds=$((execution_time % 60))  
  
echo "脚本运行时间：$minutes 分钟 $seconds 秒"
