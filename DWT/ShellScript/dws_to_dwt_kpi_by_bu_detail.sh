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
# billed:
# dned:

inbound_leadtime_sql="
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
		--case when kpicode='IE001' then 'DockWarrantDate-> Arrived Warehouse​​'
		--	 when kpicode='IE002' then 'T1 Pickup -> DockWarrantDate​'
		--	 when kpicode='WH014' then 'Arrive Warehouse -> MIGO'
		--end  as level4,		
		plant,
		dt,
		---coalesce(division_display_name,'others') as bu,
		null as bu,
		avg(lt_day_cd)value1, ---自然日
		avg(lt_day_wd)value2,	---工作日
		null as flag1, 
		null as flag2, 
		null as flag3,
		'ET0101'||kpicode as kpi_code,
		year_mon
	from dws_ie_kpi
	where year_mon>='2023-01' and kpicode in ( 'IE001','IE002','WH014')
	group by 'ET0101'||kpicode,plant,dt,year_mon
union all
	select
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'MIGO -> Putaway' as level4,		
		plant,
		dt,
		null as bu,
		avg(processtime_cd/24)value1, ---自然日
		avg(process_wd_m/24)value2,	---工作日
		null as flag1, 
		null as flag2, 
		null as flag3,
		'ET0101'||kpicode as kpi_code,
		substr(dt,1,7) as year_mon
	from dws_kpi_stock_putaway_time
	where dt>='2023-01-01' and kpicode ='WH015'
	group by 'ET0101'||kpicode,plant,dt
union all
	select
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'MIGO-> STO PGI' as level4,		
		plant,
		dt,
		null as bu,
		avg(lt_cd_hr/24)value1, ---自然日
		avg(lt_dw_hr/24)value2,	---工作日
		null as flag1, 
		null as flag2, 
		null as flag3,
		'ET0101'||kpi_no as kpi_code,
		substr(dt,1,7) as year_mon
	from dws_kpi_sto_migo2pgi
	where dt>='2023-01-01' and kpi_no ='WH016'
	group by 'ET0101'||kpi_no,plant,dt
union all
	select
		--'End to end lead time Y' as level1,
		--'Inbound Leadtime Y' as level2,
		--'Logistic Inbound Leadtime' as level3,
		-- 'STO PGI -> MIGO (STO Transportation)' as level4,		
		ship_from_plant as plant,
		dt,
		null as bu,  ---有些是空的原因是数据源变成未收货了，这部分数据应该联系xukun修复
		avg(lt_cd_hr)value1, ---自然日
		avg(lt_dw_hr)value2,	---工作日
		null as flag1, 
		null as flag2, 
		null as flag3,
		'ET0101'||kpi_no as kpi_code,
		substr(dt,1,7) as year_mon
	from dws_kpi_zc_timi
	where dt>='2023-01-01' and kpi_no ='WH018.1'
	group by 'ET0101'||kpi_no,ship_from_plant,dt

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
-----------------财务指标Inventory Charges Y
select	
	coalesce(category,plan_or_act) as plant,
	--plan_or_act as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(value_sharing) as value1,
	null as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0100'||itemcode as kpi_code,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') as year_mon
from dws_finance_cogs_sharing_detail
where itemcode='C_004' and year_mon>='202301' and plan_or_act='act'
group by 	year_mon,
	coalesce(category,plan_or_act),
	--plan_or_act,
	division_finace,
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
--E_035，Freight (BSCL)
--E_038，Facilities
--E_039，Depreciation
select 
	coalesce(category,plan_or_act) as plant,
	from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
	division_finace as bu,
	sum(value_sharing) as value1,
	null as value2,
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
    ,'E_038'
    ,'E_039')
and year_mon>='202301'
and plan_or_act='act' and value_sharing is not null
group by 	year_mon,
	coalesce(category,plan_or_act),
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
---------------------------------------------Duty Cost
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	sites as plant,
	from_unixtime(unix_timestamp(year_mon||'-01', 'yyyy-MM-dd'), 'yyyy-MM-dd') as dt,	
	coalesce(b.excel,REGEXP_REPLACE(a.division,'[^a-zA-Z]','') )as bu,
	sum(sharing_tariffs_benchmark_total_price) as value1,--分摊关税基准总价
	null as value2,
	null as flag1, 
	null as flag2, 
	null as flag3,
	'TS0400' as kpi_code,
	year_mon
from dwd_duty_by_upn_histest a
left join dwd_dim_finance_disvision_mapping_hive_disvsion b 
	on REGEXP_REPLACE(a.division,'[^a-zA-Z]','')=b.hive
where  year_mon>='2023-01'
group by 	year_mon,
	sites,
	coalesce(b.excel,REGEXP_REPLACE(a.division,'[^a-zA-Z]','') );

---------------------------------------------IE agency Cost 
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
	select 
		coalesce(category,plan_or_act) as plant,
		from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM-dd') as dt,	
		division_finace as bu,
		sum(value_sharing) as value1,
		null as value2,
		null as flag1, 
		null as flag2, 
		null as flag3,
		'TS0300' as kpi_code,
		from_unixtime(unix_timestamp(year_mon||'01', 'yyyyMMdd'), 'yyyy-MM') year_mon
	from dws_finance_cogs_sharing_detail
	where itemcode in (	'E_013','E_014','E_015','E_011','C_014')
	and year_mon>='202301'
	and plan_or_act='act'and value_sharing is not null
	group by 	year_mon,
		coalesce(category,plan_or_act),
		division_finace;
"
clearance_leadtime_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

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
		avg(distirib_custom_cd)value1, ---自然日
		avg(distirib_custom_wd)value2,	---工作日
		'\u5206\u62e8'  as flag1, --分拨
		'非起搏器' as flag2, --是否起搏器
		'PVG' as flag3, --到货机场
		'ET0102'||'IE006.1' as kpi_code,
		substr(customclearance_date,1,7) as year_mon
	from dwd_outbound_distribution --sh部分
	where dt='$max_dt_fenbo' 
	and customclearance_date is not null 
	and distributedoc_receivedate is not null --收到清单时间
	group by 'ET0102'||'IE006.1',
		substr(customclearance_date,1,10),
		substr(customclearance_date,1,7);
--------------------------------------------------------------------------舱单确认时间->COO Draft收到时间​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(coodraft_receiveddate,1,10) as dt,  --COO Draft收到时间​
	null as bu, 
	avg(get_json_object(jsons, '$.dock_coodraft_cd'))value1, ---自然日
	avg(get_json_object(jsons, '$.dock_coodraft_wd'))value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE013' as kpi_code,
		substr(coodraft_receiveddate,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(coodraft_receiveddate,1,7) >='2023-01'
and dockwarrantdate is not null
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(coodraft_receiveddate,1,10),
		substr(coodraft_receiveddate,1,7);
--------------------------------------------------------------------------COO正本收到时间->入库时间​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(intoinventorydate,1,10) as dt,  --入库时间​
	null as bu, 
	avg(get_json_object(jsons, '$.coocer_invent_cd'))value1, ---自然日
	avg(get_json_object(jsons, '$.coocer_invent_wd'))value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE014' as kpi_code,
	substr(intoinventorydate,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(intoinventorydate,1,7) >='2023-01'
and coocertificate_receiveddate is not null ---COO正本收到时间
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(intoinventorydate,1,10),
		substr(intoinventorydate,1,7);
--------------------------------------------------------------------------入库时间->海关一放时间​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(customrelease_1,1,10) as dt,  --海关一放时间​
	null as bu, 
	avg(invent_cust1_cd)value1, ---自然日
	avg(invent_cust1_wd)value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE007' as kpi_code,
	substr(customrelease_1,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(customrelease_1,1,7) >='2023-01'
and intoinventorydate is not null ---入库时间​
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(customrelease_1,1,10),
		substr(customrelease_1,1,7);
--------------------------------------------------------------------------海关一放时间->收到仓库中文标签照片时间​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(chineselabelpicturereceiveddate,1,10) as dt,  --收到仓库中文标签照片时间​
	null as bu, 
	avg(cust1_chinesepicture_cd)value1, ---自然日
	avg(cust1_chinesepicture_wd)value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE008' as kpi_code,
	substr(chineselabelpicturereceiveddate,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(chineselabelpicturereceiveddate,1,7) >='2023-01'
and customrelease_1 is not null ---海关一放时间
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(chineselabelpicturereceiveddate,1,10),
		substr(chineselabelpicturereceiveddate,1,7);
--------------------------------------------------------------------------收到仓库中文标签照片时间->商检查验时间​​
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(commodityinspection_date,1,10) as dt,  --商检查验时间​​
	null as bu, 
	avg(chinesepicture_commodity_cd)value1, ---自然日
	avg(chinesepicture_commodity_wd)value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE009' as kpi_code,
	substr(commodityinspection_date,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(commodityinspection_date,1,7) >='2023-01'
and chineselabelpicturereceiveddate is not null ---收到仓库中文标签照片时间
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(commodityinspection_date,1,10),
		substr(commodityinspection_date,1,7);

--------------------------------------------------------------------------商检查验时间->实际检测时间(起搏器)
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(actualtest_date,1,10) as dt,  --实际检测时间(起搏器)​
	null as bu, 
	avg(commod_act_cust2_cd)value1, ---自然日
	avg(commod_act_cust2_wd)value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE010' as kpi_code,
	substr(actualtest_date,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(actualtest_date,1,7) >='2023-01'
and commodityinspection_date is not null ---商检查验时间​​
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(actualtest_date,1,10),
		substr(actualtest_date,1,7);

--------------------------------------------------------------------------实际检测时间->两证完成(起搏器)
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(ciq_signcompletiondate,1,10) as dt,  --两证完成(起搏器)​
	null as bu, 
	avg(act_ciq_cd)value1, ---自然日
	avg(act_ciq_wd)value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE011' as kpi_code,
	substr(ciq_signcompletiondate,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(ciq_signcompletiondate,1,7) >='2023-01'
and actualtest_date is not null ---实际检测时间(起搏器)​
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(ciq_signcompletiondate,1,10),
		substr(ciq_signcompletiondate,1,7);

--------------------------------------------------------------------------商检查验时间->海关二放时间(非起搏器)
insert overwrite table dwt_kpi_by_bu_detail partition(kpi_code,year_mon)
select 
	coalesce(destination_wh,'D835')as plant,
	substr(customrelease_2,1,10) as dt,  --海关二放时间(非起搏器)
	null as bu, 
	avg(commod_act_cust2_cd)value1, ---自然日
	avg(commod_act_cust2_wd)value2,	---工作日
	distribution_status as flag1, --是否分拨
	case when category_code='2' then '起搏器' else '非起搏器' end as flag2, --是否起搏器
	airport as flag3, --到货机场
	'ET0102'||'IE010' as kpi_code,
	substr(customrelease_2,1,7) as year_mon
from dws_iekpi_e2e_aws 
where outbound_yr =substr('$sync_date',1,4)
and substr(customrelease_2,1,7) >='2023-01'
and commodityinspection_date is not null ---商检查验时间​​
group by coalesce(destination_wh,'D835'),
		distribution_status,
		case when category_code='2' then '起搏器' else '非起搏器' end,
		airport,
		substr(customrelease_2,1,10),
		substr(customrelease_2,1,7);
"
sto_E_022_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------sto摊分


"
SCRAP_COST_E_018_sql="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--------------------------------------------------------------------------销毁申请摊分

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
	echo "$inbound_leadtime_sql"
	$hive -e "$inbound_leadtime_sql"
	#$hive -e "$delete_delivery_tmp"
elif [ "$1"x = "finance"x ];then
	echo "dws $1 only run"	
	echo "$finance_sql"
	$hive -e "$finance_sql"
	#$hive -e "$delete_inbound_tmp"
elif [ "$1"x = "duty_by_upn_his"x ];then
	echo "dws $1 only run"	
	echo "$duty_by_upn_histest_sql"
	$hive -e "$duty_by_upn_histest_sql"
	#$hive -e "$delete_onhand_E_020_tmp"
elif [ "$1"x = "clearance_leadtime"x ];then
	echo "$clearance_leadtime_sql "	
	$hive -e "$clearance_leadtime_sql"
# elif [ "$1"x = "E_018"x ];then
# 	echo "dws $1 only run"	
# 	$hive -e "$SCRAP_COST_E_018_sql"
# elif [ "$1"x = "workorder"x ];then
# 	echo "dws $1 only run"	
# 	$hive -e "$WorkOrder_sql"
# elif [ "$1"x = "Facilities"x ];then
# 	echo "dws $1 only run"	
# 	$hive -e "$Facilities_sql"
else
    echo "--------------------------------------------------------------------------inbound_leadtime_sql"
    $hive -e "$inbound_leadtime_sql"
    echo "--------------------------------------------------------------------------finance sql"
    $hive -e "$finance_sql"
    echo "--------------------------------------------------------------------------duty_by_upn_histest_sql"
    $hive -e "$duty_by_upn_histest_sql"
    echo "--------------------------------------------------------------------------clearance_leadtime"
    $hive -e "$clearance_leadtime_sql"
	# echo "--------------------------------------------------------------------------E_018"
	# $hive -e "$SCRAP_COST_E_018_sql"
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