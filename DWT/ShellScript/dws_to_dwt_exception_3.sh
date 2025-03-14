#!/bin/bash
# Function:
#   sync up dws_to_dwt_kpi_by_bu_detail_PO_DN 
# History:
# 2024-01-18    Sway   v1.0    init

# 参数
target_db_name='opsdw' # 目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径

if [ -n "$1" ] ;then 
    sync_date=$1
	end_date=$1
else
    sync_date=$(date  +%F)
	end_date=$(date  +%F)
fi

# 获取下个月的日期（加上一个月）  
next_month_date=$(date -d "$current_date +1 month" +"%F") 

echo "start syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} : ${sync_date[year]}"
dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | tail -n 1 | awk -F'=' '{print $NF}'`
ods_mdm_materialmaster_marc_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_mdm_materialmaster_marc | tail -n 1 | awk -F'=' '{print $NF}'`


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
set hive.exec.reducers.max=8;
set mapred.reduce.tasks=8;
add jar /user/hive/numDay-1.0-SNAPSHOT.jar;
create temporary function myudf as 'org.example.Nmu';

drop table if exists tmp_dwd_marc_exception;
create table  tmp_dwd_marc_exception stored as orc as
--P1 marc_s
select 
a.material,profitcenter,plant,division_type,cast(b.count_plant as string) para1,'' para2,
case when b.count_plant=4 then 'pass' else 'error' END exception_type,
'W-C-MATNR-01' exception_code,dt
from tmp_materialmaster_marc a join (
select material,COUNT(DISTINCT plant) count_plant  from tmp_materialmaster_marc
WHERE Plant IN ('D835', 'D836', 'D837', 'D838') and division_type ='BSC'
group by material
--HAVING COUNT(DISTINCT profitcenter) > 1
) b on a.material=b.material   --p1 plant不一致异常
where a.Plant IN ('D835', 'D836', 'D837', 'D838')
union all
select 
a.material,profitcenter,plant,division_type,cast(b.count_plant as string) para1,'' para2,
case when b.count_plant=2 then 'pass' else 'error' END exception_type,
'W-C-MATNR-01' exception_code,dt
from tmp_materialmaster_marc a join (
select material,COUNT(DISTINCT plant) count_plant  from tmp_materialmaster_marc
WHERE Plant IN ('D835','D838') and division_type ='CRM'
group by material
--HAVING COUNT(DISTINCT profitcenter) > 1
) b on a.material=b.material   --p1 plant不一致异常
where a.Plant IN ('D835','D838')
union all 
--P8   marc_s
select 
material,profitcenter,plant,division_type,loadinggroup para1,'' para2,
case when loadinggroup<>'0001' then 'error' else 'pass' END exception_type,
'E-A-LADGE-01' exception_code,dt
from tmp_materialmaster_marc where division_type='BSC'   --p8 loading group异常
union all 
select 
material,profitcenter,plant,division_type,loadinggroup para1,'' para2,
case when loadinggroup<>'Z001' then 'error' else 'pass' END exception_type,
'E-A-LADGE-01' exception_code,dt
from tmp_materialmaster_marc where division_type='CRM'  --p8 loading group异常
union all 
--P6 marc_s
select 
material,profitcenter,plant,division_type,storagelocationep para1,psmatlstatus para2,
case when psmatlstatus<>'88' then 'warning' else 'pass' END  exception_type,
'W-A-LGFSB-01' exception_code,dt
from tmp_materialmaster_marc where storagelocationep='ZEOL'  --p6 PLCP=S6, S7, MMPP<>88
union all 
select 
material,profitcenter,plant,division_type,storagelocationep para1,psmatlstatus para2,
case when psmatlstatus<>'10' then 'warning' else 'pass' END exception_type,
'W-A-LGFSB-02' exception_code,dt
from tmp_materialmaster_marc where storagelocationep='ZNPL'   --p6 PLCP=S3, MMPP<>10
union all 
--P12 13 marc_s
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case 
	when c.st in ('00','40')  then 'pass' 
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where a.storagelocationep ='ZEOL' and a.psmatlstatus ='88' --4918
union all
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case 
	when c.st in ('03','40')  then 'pass' 
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_crm b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where a.storagelocationep ='ZEOL' and a.psmatlstatus ='88'
union all 
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when  c.st ='20' then 'pass'
	when  c.st ='00' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where  a.psmatlstatus in ('00','10')
union all 
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when  c.st ='20' then 'pass'
	when  c.st ='03' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_crm b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where  a.psmatlstatus in ('00','10')
union all 
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when a.psmatlstatus ='00' and c.st ='00' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_noapproved b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where  a.storagelocationep ='CLIN'
union all
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when a.psmatlstatus ='00' and c.st ='03' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_nomr_crm b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where  a.storagelocationep ='CLIN'
union all
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when a.psmatlstatus ='10' and c.st ='20' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_noapproved b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where  a.storagelocationep ='ZNPL'
union ALL 
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when a.psmatlstatus ='10' and c.st ='20' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_nomr_crm b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
where  a.storagelocationep ='ZNPL'
union all
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when a.psmatlstatus ='88' and c.st ='40' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_noapproved b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
union all 
select 
a.material,profitcenter,plant,division_type,psmatlstatus para1,c.st para2,
case  
	when a.psmatlstatus ='88' and c.st ='40' then 'pass'
else 'error' end exception_type,
'E-A-MMPP_DCHAIN-01' exception_code,a.dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_nomr_crm b 
on a.material =b.material_code
LEFT JOIN tmp_ods_dq_mvke c on a.material =c.material
union all 
--P19 marc_s
select 
a.material,profitcenter,plant,division_type,cast(b.count_proc as string) para1,'' para2,
case when b.count_proc>1 then 'error' else 'pass' END exception_type,
'E-C-PRCTR-01' exception_code,dt
from tmp_materialmaster_marc a join (
select material,COUNT(DISTINCT profitcenter) count_proc  from tmp_materialmaster_marc
group by material
--HAVING COUNT(DISTINCT profitcenter) > 1
) b on a.material=b.material   --p19 plant不一致异常
union all 
--P36 marc_s
select 
material,profitcenter,plant,division_type,specproctype para1,'' para2,
case when specproctype<>'S9' then 'error' else 'pass' end exception_type,
'E-I-SOBSK-01' exception_code,dt
from tmp_materialmaster_marc   --p36 S9异常
union all 
select 
material,profitcenter,plant,division_type,specproctype para1,'' para2,
case when specproctype<>'S9' then 'error' else 'pass' end exception_type,
 'E-I-SOBSK-01' exception_code,dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material b 
on a.material=b.material_code
where a.plant in ('D875', 'D535', 'D505') --p36 S9异常
union all
--P37 marc_s
select 
material,profitcenter,plant,division_type,posttoinspstk para1,'' para2,
case when posttoinspstk<>'X' then 'error' else 'pass' END exception_type,
'E-I-INSMK-01' exception_code,dt
from tmp_materialmaster_marc   --p37 QI Flag异常
union all
--P14 marc_s SLI
select 
material,profitcenter,plant,division_type,SourceList para1,'' para2,
case when SourceList='X' then 'error' else 'pass' end exception_type,
'E-A-SLI-02' exception_code,dt
from tmp_materialmaster_marc where plant='D838'  --p14 D838 SLI 异常
union all
select 
material,profitcenter,plant,division_type,SourceList para1,'' para2,
case when a.SourceList is null then 'error' else 'pass' end  exception_type,
'W-C-SLI_LS-02' exception_code,dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material_noapproved b 
on a.material=b.material_code
where a.plant in ('D835', 'D836', 'D837')   --p14 LS <> Approved, SLI 异常
union all
select 
material,profitcenter,plant,division_type,SourceList para1,'' para2,
case when a.SourceList ='X' then 'error' else 'pass' end  exception_type,
'W-C-SLI_LS-01' exception_code,dt
from tmp_materialmaster_marc a 
join tmp_dwd_dim_material b 
on a.material=b.material_code
where a.plant in ('D835', 'D836', 'D837')
union all
select 
material,profitcenter,plant,division_type,SourceList para1,'' para2,
case when SourceList ='X' then 'error' else 'pass' end  exception_type,
'E-A-SLI-01' exception_code,dt
from tmp_materialmaster_marc 
where plant in ('D835', 'D836', 'D837') and division_type='CRM';

"


delete_tmp="
drop table tmp_materialmaster_marc;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} .................."