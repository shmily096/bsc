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

drop table if exists tmp_dws_eeo_Excess;
create table  tmp_dws_eeo_Excess stored as orc as 
	SELECT  material ,profitcenter ,versions,eeo_flag,file_name,small_version,
		sum(unrestricted) as unrestricted,
		sum(qualinspect) as qualinspect,
		sum(blockedstock) as blockedstock,
		sum(totalinventory ) as totalinventory,
		sum(reserve) as reserve
	from dws_eeo_detail where eeo_flag='Excess'
	group by 
		material ,profitcenter ,versions,eeo_flag,file_name,small_version;

drop table if exists tmp_dws_eeo_Expired;
create table  tmp_dws_eeo_Expired stored as orc as 
	SELECT  material ,profitcenter ,versions,eeo_flag,file_name,small_version,
		sum(unrestricted) as unrestricted,
		sum(qualinspect) as qualinspect,
		sum(blockedstock) as blockedstock,
		sum(totalinventory ) as totalinventory,
		sum(reserve) as reserve
	from dws_eeo_detail where eeo_flag='Expired'
	group by 
		material ,profitcenter ,versions,eeo_flag,file_name,small_version;

drop table if exists tmp_dws_eeo_Obsolete;
create table  tmp_dws_eeo_Obsolete stored as orc as 
	SELECT material ,profitcenter ,versions,eeo_flag,file_name,small_version,
		sum(unrestricted) as unrestricted,
		sum(qualinspect) as qualinspect,
		sum(blockedstock) as blockedstock,
		sum(totalinventory ) as totalinventory,
		sum(reserve) as reserve
	from dws_eeo_detail where eeo_flag='Obsolete'
	group by 
		material ,profitcenter ,versions,eeo_flag,file_name,small_version;

drop table if exists tmp_dws_eeo_std_reval;
create table  tmp_dws_eeo_std_reval stored as orc as 
	SELECT cate,material ,profitcenter ,versions,eeo_flag,file_name,small_version,
		sum(pricechange) as pricechange
	from (
	SELECT cate,material ,profitcenter ,pricechange
	,versions ,'std_reval' as eeo_flag,file_name ,if(small_version=''  ,'V0', small_version) AS small_version 
	,DENSE_RANK()over(partition by versions order by if(small_version='', '0', replace(small_version,'V','')) DESC ) rn
	from dwd_finance_std_reval_detail
	where versions||dt=(select versions||max(dt) from dwd_finance_std_reval_detail group by versions)
	)xx
	where rn =1
	group by 
		cate,material ,profitcenter ,versions,eeo_flag,file_name,small_version;

drop table if exists tmp_dws_eeo;
create table  tmp_dws_eeo stored as orc as 
select 
	xx.material ,
	xx.profitcenter ,
	xx.versions,
	xx.eeo_flag,
	xx.file_name,
	xx.small_version,
	xx.unrestricted,
	xx.qualinspect,
	xx.blockedstock,
	xx.totalinventory,
	xx.reserve,
	coalesce(std.pricechange,0) as pricechange,
	DATE_FORMAT(add_months(substring(xx.versions,1,4)||'-'||substring(xx.versions,5,2)||'-'||'01',-1),'yyyyMM') as last_version
from (
select
	material ,
	profitcenter ,
	versions,
	eeo_flag,
	file_name,
	small_version,
	unrestricted,
	qualinspect,
	blockedstock,
	totalinventory,
	reserve
from tmp_dws_eeo_Excess
union all 
select
	material ,
	profitcenter ,
	versions,
	eeo_flag,
	file_name,
	small_version,
	unrestricted,
	qualinspect,
	blockedstock,
	totalinventory,
	reserve
from tmp_dws_eeo_Expired
union all 
select
	material ,
	profitcenter ,
	versions,
	eeo_flag,
	file_name,
	small_version,
	unrestricted,
	qualinspect,
	blockedstock,
	totalinventory,
	reserve
from tmp_dws_eeo_Obsolete
)xx
left join tmp_dws_eeo_std_reval std 
	on xx.material=std.material 
	and xx.profitcenter=std.profitcenter 
	and xx.eeo_flag=std.cate
	and xx.versions=std.versions;

drop table if exists tmp_dws_eeo_last;
create table  tmp_dws_eeo_last stored as orc as 
select
	a.material ,
	a.profitcenter ,
	a.file_name,
	a.small_version,
	a.unrestricted,
	a.qualinspect,
	a.blockedstock,
	a.totalinventory,
	a.reserve,
	a.pricechange,
	b.reserve as last_reserve,
	a.reserve-coalesce(b.reserve,0)-a.pricechange as finall_reserve,
	a.eeo_flag,
	a.versions
from tmp_dws_eeo a
left join tmp_dws_eeo b 
	on a.material=b.material
	and a.profitcenter=b.profitcenter
	and a.versions=b.last_version;

insert overwrite table opsdw.dws_eeo partition(eeo_flag,versions)
select
	a.material ,
	a.profitcenter ,
	a.file_name,
	a.small_version,
	a.unrestricted,
	a.qualinspect,
	a.blockedstock,
	a.totalinventory,
	a.reserve,
	a.pricechange,
	a.last_reserve,
	a.finall_reserve,
	a.eeo_flag,
	a.versions
from tmp_dws_eeo_last a
"
delete_tmp="
drop table tmp_dws_eeo;
drop table tmp_dws_eeo_Excess;
drop table tmp_dws_eeo_Expired;
drop table tmp_dws_eeo_Obsolete;
drop table tmp_dws_eeo_std_reval;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
#echo "two $delete_tmp"
#$hive -e "$delete_tmp"
echo "End syncing dws_kpi_zc_timi data into DWS layer on ${sync_date} .................."