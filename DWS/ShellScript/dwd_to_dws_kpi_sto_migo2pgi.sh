#!/bin/bash
# Function:
#   sync up dws_kpi_zc_timi 
# History:
# 2021-07-08    Donny   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

declare -A sync_date=$(date +'([day]=%F [year]=%Y [month]=%m)')
yesterday=$(date -d '-1 day' +%F)
year_month=$(date  +'%Y-%m')

echo "start syncing dws_kpi_zc_timi data into DWS layer on ${sync_date[month]} : ${sync_date[year]}"

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
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

drop table if exists tmp_sto_migo2pgi;
create table tmp_sto_migo2pgi stored as orc as 
 select 
	plant, upn, bt, inventorystatus, sapinbounddn, workorderno, batch, inbounddn,  qty, 
	outbounddn, supplier, id, 
	inboundtime, 
	unix_timestamp(inboundtime) as inboundtime_unix,
	outboundtime,
	unix_timestamp(outboundtime) as outboundtime_unix,
	localizationfinishtime,
	coalesce(sto.is_pacemaker,cast(pa.is_pacemaker as string))as is_pacemaker,
	coalesce(sto.distribution_properties,pa.distribution_properties) as distribution_properties,
	unix_timestamp(localizationfinishtime) as localizationfinishtime_unix
 FROM dwd_sto_migo2pgi sto
 left join dwd_pacemaker_list pa on sto.upn=pa.material
 --where dt=(select max(dt) from dwd_sto_migo2pgi)
 where dt  >= date_add('${sync_date[day]}',-93)
       and dt  <= '${sync_date[day]}';

insert overwrite table opsdw.dws_kpi_sto_migo2pgi partition(kpi_no,dt)
select
	plant, upn, bt, inventorystatus, sapinbounddn, workorderno, 
	batch, inbounddn, 
	outbounddn, supplier, id, 
	inboundtime, 
	outboundtime,
	localizationfinishtime
	, case when localizationfinishtime is not null then qty end as qty
	, CAST(((case when localizationfinishtime is not null then localizationfinishtime_unix end) - (case when localizationfinishtime is not null then inboundtime_unix end))/(60 * 60) AS float)  as lt_cd_hr --运单发出到收货一共多少小时
	, myudf((case when localizationfinishtime is not null then inboundtime end),(case when localizationfinishtime is not null then localizationfinishtime end))*24  as no_work_hr    --其中非工作日多少小时
	, case when CAST(((case when localizationfinishtime is not null then localizationfinishtime_unix end) - (case when localizationfinishtime is not null then inboundtime_unix end))/(60 * 60) AS float)
				 - myudf((case when localizationfinishtime is not null then inboundtime end),(case when localizationfinishtime is not null then localizationfinishtime end))*24 <0 then 0
		   else CAST(((case when localizationfinishtime is not null then localizationfinishtime_unix end) - (case when localizationfinishtime is not null then inboundtime_unix end))/(60 * 60) AS float)
				 - myudf((case when localizationfinishtime is not null then inboundtime end),(case when localizationfinishtime is not null then localizationfinishtime end))*24 
		   end as lt_dw_hr --剔除非工作日多少小时	
	,is_pacemaker
	,distribution_properties
	, 'WH016.0' as kpi_no
	, date_format(localizationfinishtime,'yyyy-MM-dd')as dt
from tmp_sto_migo2pgi
where localizationfinishtime is not null;
insert overwrite table opsdw.dws_kpi_sto_migo2pgi partition(kpi_no,dt)
select
	plant, upn, bt, inventorystatus, sapinbounddn, workorderno, batch, inbounddn,  
	outbounddn, supplier, id, 
	inboundtime, 
	outboundtime,
	localizationfinishtime
	, case when outboundtime is not null then qty end as  qty
	, CAST(((case when outboundtime is not null then outboundtime_unix end) - (case when outboundtime is not null then localizationfinishtime_unix end))/(60 * 60) AS float)  as lt_cd_hr --运单发出到收货一共多少小时
	, myudf((case when outboundtime is not null then localizationfinishtime end),(case when outboundtime is not null then outboundtime end))*24  as no_work_hr    --其中非工作日多少小时
	, case when CAST(((case when outboundtime is not null then outboundtime_unix end) - (case when outboundtime is not null then localizationfinishtime_unix end))/(60 * 60) AS float)
				 - myudf((case when outboundtime is not null then localizationfinishtime end),(case when outboundtime is not null then outboundtime end))*24 <0 then 0
		   else CAST(((case when outboundtime is not null then outboundtime_unix end) - (case when outboundtime is not null then localizationfinishtime_unix end))/(60 * 60) AS float)
				 - myudf((case when outboundtime is not null then localizationfinishtime end),(case when outboundtime is not null then outboundtime end))*24 
		   end as lt_dw_hr --剔除非工作日多少小时	
	,is_pacemaker
	,distribution_properties
	, 'WH016.1' as kpi_no
	, date_format(outboundtime,'yyyy-MM-dd')as dt
	from tmp_sto_migo2pgi
	where outboundtime is not null;
insert overwrite table opsdw.dws_kpi_sto_migo2pgi partition(kpi_no,dt)
select
	plant, upn, bt, inventorystatus, sapinbounddn, workorderno, batch, inbounddn,  
	outbounddn, supplier, id, 
	inboundtime, 
	outboundtime,
	localizationfinishtime
	, case when outboundtime is not null then qty end as  qty
	, CAST(((case when outboundtime is not null then outboundtime_unix end) - (case when outboundtime is not null then inboundtime_unix end))/(60 * 60) AS float)  as lt_cd_hr --运单发出到收货一共多少小时
	, myudf((case when outboundtime is not null then inboundtime end),(case when outboundtime is not null then outboundtime end))*24  as no_work_hr    --其中非工作日多少小时
	, case when CAST(((case when outboundtime is not null then outboundtime_unix end) - (case when outboundtime is not null then inboundtime_unix end))/(60 * 60) AS float)
				 - myudf((case when outboundtime is not null then inboundtime end),(case when outboundtime is not null then outboundtime end))*24 <0 then 0
		   else CAST(((case when outboundtime is not null then outboundtime_unix end) - (case when outboundtime is not null then inboundtime_unix end))/(60 * 60) AS float)
				 - myudf((case when outboundtime is not null then inboundtime end),(case when outboundtime is not null then outboundtime end))*24 
		   end as lt_dw_hr --剔除非工作日多少小时	
	,is_pacemaker
	,distribution_properties
	, 'WH016' as kpi_no
	, date_format(outboundtime,'yyyy-MM-dd')as dt
	from tmp_sto_migo2pgi
	where outboundtime is not null;
"
delete_tmp="
drop table tmp_sto_migo2pgi;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
echo "four $delete_tmp"
$hive -e "$delete_tmp"

echo "End syncing dws_kpi_sto_migo2pgi data into DWS layer on ${sync_date} .................."