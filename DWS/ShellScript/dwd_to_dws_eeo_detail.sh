#!/bin/bash
# Function:
#   sync up dws_kpi_zc_timi 
# History:
# 2021-07-08    Donny   v1.0    init
# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi
echo "start syncing dws_kpi_zc_timi data into DWS layer on ${sync_date} : ${sync_date[year]}"
excess_sql="
-- 参数
use ${target_db_name};
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100;
set hive.exec.max.dynamic.partitions=100;
drop table if exists tmp_dws_eeo_detail_Excess;
create table  tmp_dws_eeo_detail_Excess stored as orc as 
	SELECT 
		plant ,
		material ,
		profitcenter ,
		unrestricted ,
		qualinspect ,
		blockedstock ,
		totalinventory ,
		extk ,
		reserve ,
		versions ,
		file_name ,
		if(small_version=''  ,'V0', small_version) AS small_version ,
		IF(fieldtype='Whs','WH','CSGN') as locations,
		'Excess' as eeo_flag,	
		dt,
		DENSE_RANK()over(partition by versions order by if(small_version='', '0', replace(small_version,'V','')) DESC ) rn
	from dwd_finance_excess_reserve_detail
	where dt='$sync_date';
insert overwrite table opsdw.dws_eeo_detail partition(eeo_flag,versions)
----过剩
select
	plant ,
	material ,
	profitcenter ,
	unrestricted ,
	qualinspect ,
	blockedstock ,
	totalinventory ,
	extk ,
	reserve ,	
	file_name ,
	small_version,
	locations,
	null as pul,
	null as io,
	dt,
	eeo_flag,
	versions 
from tmp_dws_eeo_detail_Excess
where rn=1;
"
expired_sql="
use ${target_db_name};
set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
drop table if exists tmp_dws_eeo_detail_Expired;
create table  tmp_dws_eeo_detail_Expired stored as orc as 
SELECT 
		plant ,
		material ,
		profitcenter ,
		unrestricted ,
		qualinspect ,
		blockedstock ,
		totalinventory ,
		stdcostrmb as extk ,
		inventoryreserve as reserve ,
		versions ,
		file_name ,
		if(small_version=''  ,'V0', small_version) AS small_version ,
		locations,
		pul,
		io,
		'Expired' as eeo_flag,	
		dt,
		DENSE_RANK()over(partition by versions order by if(small_version='', '0', replace(small_version,'V','')) DESC ) rn
	from dwd_finance_expired_reserve_detail
	where  dt='$sync_date';
insert overwrite table opsdw.dws_eeo_detail partition(eeo_flag,versions)
----过期
select
	plant ,
	material ,
	profitcenter ,
	unrestricted ,
	qualinspect ,
	blockedstock ,
	totalinventory ,
	extk ,
	reserve ,	
	file_name ,
	small_version,
	locations,
	pul,
	io,
	dt,
	eeo_flag,
	versions 
from tmp_dws_eeo_detail_Expired
where rn=1;
"
Obsolete_sql="
use ${target_db_name};
set mapreduce.job.queuename = default;
set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 100000;
set hive.exec.max.dynamic.partitions = 100000;
drop table if exists tmp_dws_eeo_detail_Obsolete;
create table  tmp_dws_eeo_detail_Obsolete stored as orc as 
	SELECT 
		plant ,
		material ,
		profitcenter ,
		unrestricted ,
		qualinspect ,
		blockedstock ,
		totalinventory ,
		stdcostrmb as extk ,
		reserve ,
		versions ,
		file_name ,
		if(small_version=''  ,'V0', small_version) AS small_version ,
		IF(fieldtype='Whs','WH','CSGN') as locations,
		endopul as pul,
		io,
		'Obsolete' as eeo_flag,	
		dt,
		DENSE_RANK()over(partition by versions order by if(small_version='', '0', replace(small_version,'V','')) DESC ) rn
	from dwd_finance_obsolete_reserve_detail
	where  dt='$sync_date';
insert overwrite table opsdw.dws_eeo_detail partition(eeo_flag,versions)
----报废
select
	plant ,
	material ,
	profitcenter ,
	unrestricted ,
	qualinspect ,
	blockedstock ,
	totalinventory ,
	extk ,
	reserve ,	
	file_name ,
	small_version,
	locations,
	pul,
	io,
	dt,
	eeo_flag,
	versions 
from tmp_dws_eeo_detail_Obsolete
where rn=1;
"
delete_Excess="
drop table tmp_dws_eeo_detail_Excess;
"
delete_Expired="
drop table tmp_dws_eeo_detail_Expired;
"
delete_Obsolete="
drop table tmp_dws_eeo_detail_Obsolete;
"
# 2. 执行加载数据SQL
if [ "$1"x = "Excess"x ];then
	echo "$excess_sql"
	$hive -e "$excess_sql"
	$hive -e "$delete_Excess"
elif [ "$1"x = "expired"x ];then
	echo "$expired_sql"
	$hive -e "$expired_sql"
	$hive -e "$delete_Expired"
elif [ "$1"x = "Obsolete"x ];then
	echo "$Obsolete_sql"
	$hive -e "$Obsolete_sql"
	$hive -e "$delete_Obsolete"
else
    echo "please give ok date "
fi
echo "End syncing dws_kpi_zc_timi data into DWS layer on ${sync_date} .................."