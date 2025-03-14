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
ods_dq_mara_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_dq_mara | tail -n 1 | awk -F'=' '{print $NF}'`
# ods_dq_mara_m2dt=`hdfs dfs -ls /bsc/opsdw/ods/ods_dq_mara | tail -n 2 | head -n 1 | awk -F'=' '{print $NF}'`
dwd_dq_mara_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dq_mara | tail -n 1 | awk -F'=' '{print $NF}'`
dwd_dq_mara_m2dt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dq_mara | tail -n 2 | head -n 1 | awk -F'=' '{print $NF}'`

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

drop table if exists tmp_dwd_dq_mara;
create table  tmp_dwd_dq_mara stored as orc as 
SELECT *
FROM opsdw.dwd_dq_mara
where dt='$dwd_dq_mara_maxdt'
and material not in (select DISTINCT material from opsdw.ods_dq_mara where dt='$ods_dq_mara_maxdt' and material is not null)
union all 
SELECT * FROM opsdw.ods_dq_mara where dt='$ods_dq_mara_maxdt';

insert overwrite table dwd_dq_mara partition(dt)
SELECT clt, last_chg, changed_by, ms, st, material, old_material_no, s, no, product_hierarchy, 
slife, ean_upn, matl_group, mtyp, bun, l_o, ct, basic_material, sc, temp, ct_1, taxci, uom, orig,
bmr, serial_no_profile, drug_indicator, srai_eligibility, capital_equipment_relevant, repairable, 
service_part_type, fseai_eligibility, material_description, insertdt, 
'$ods_dq_mara_maxdt' dt
FROM tmp_dwd_dq_mara;



drop table if exists tmp_dq_P5;
create table  tmp_dq_P5 stored as orc as 
--P5
select 
a.material,b.profitcenter ,a.plnt,b.division_type ,b.plant para1,b.storagelocationep para2,
case 
 when a.plnt='D838' and b.plant ='D835' and b.storagelocationep not in ('LP05','BF05') then 'error'
 when a.plnt<>'D838' and b.plant ='D835' and b.storagelocationep ='LP05' then 'error'
 when a.plnt='D835' and b.plant ='D835' and b.storagelocationep not in ('0001','BF01') then 'error'
 when a.plnt<>'D835' and b.plant ='D835' and b.storagelocationep ='0001' then 'error'
 when a.plnt='D837' and b.plant ='D837' and b.storagelocationep <>'Y001' then 'error'
 when a.plnt<>'D837' and b.plant ='D837' and b.storagelocationep ='Y001' then 'error'
 when a.plnt='D836' and b.plant ='D836' and b.storagelocationep <>'Y001' then 'error'
 when a.plnt<>'D836' and b.plant ='D836' and b.storagelocationep ='Y001' then 'error'
else 'pass' end exception_type,
'W-A-DWERK_LGFSB-01' exception_code,a.dt
from tmp_ods_dq_mvke a 
join tmp_materialmaster_marc b 
on a.material=b.material 
where a.sorg='CN10' and b.division_type='BSC'
and b.storagelocationep in ('LP05','BF05','0001','BF01','Y001','B001')
union all 
select 
a.material,b.profitcenter,a.plnt,b.division_type,b.plant para1,b.storagelocationep para2,
case 
 when a.plnt='D838' and b.plant ='D835' and b.storagelocationep not in ('LP05','BF05') then 'error'
 when a.plnt<>'D838' and b.plant ='D835' and b.storagelocationep ='LP05' then 'error'
 when a.plnt='D835' and b.plant ='D835' and b.storagelocationep not in ('0001','BF01') then 'error'
 when a.plnt<>'D835' and b.plant ='D835' and b.storagelocationep ='0001' then 'error'
 when a.plnt='DG80' and b.plant ='DG80' and b.storagelocationep <>'B001' then 'error'
 when a.plnt<>'DG80' and b.plant ='DG80' and b.storagelocationep ='B001' then 'error'
else 'pass' end exception_type,
'W-A-DWERK_LGFSB-01' exception_code,a.dt
from tmp_ods_dq_mvke a 
join tmp_materialmaster_marc b 
on a.material=b.material 
where a.sorg='CN50' and b.division_type='CRM'
and b.storagelocationep in ('LP05','BF05','0001','BF01','Y001','B001');

drop table if exists tmp_dq_P18;
create table  tmp_dq_P18 stored as orc as 
--P18
select a.material,a.profitcenter,a.plant,a.division_type ,b.product_hierarchy para1,'' para2, 
case when c.value is null then 'error'else 'pass' end exception_type,
'W-A-PRODH_PRCTR-01' exception_code,a.dt
from tmp_materialmaster_marc a
join tmp_ods_dq_mvke b 
on a.material =b.material
left JOIN tmp_ods_dq_zv002 c
on a.profitcenter =c.profit_ctr and SUBSTRING(b.product_hierarchy,0,2)=c.product_hierarchy
where plant in ('D835', 'D836', 'D837', 'D838') and a.profitcenter is not null;

drop table if exists tmp_dq_P23;
create table  tmp_dq_P23 stored as orc as
--P23 P24
select a.material, a.mg1,
case when a.mg1='002' then 'pass' else 'error' end exception_type,
'E-A-ZV001_MVGR1-01' exception_code,a.dt
from tmp_ods_dq_mvke a 
join tmp_ods_dq_zv001 b 
on a.material = b.material
union 
select a.material, a.mg1,
case when b.material is null then 'error' else 'pass' end exception_type,
'E-A-ZV001_MVGR1-01' exception_code,a.dt
from tmp_ods_dq_mvke a 
left join tmp_ods_dq_zv001 b 
on a.material = b.material
where a.mg1='002';

drop table if exists tmp_dq_P25;
create table  tmp_dq_P25 stored as orc as
--P25 P27
select a.material,a.profitcenter,a.plant,a.division_type ,b.MG2 para1,b.MG4 para2, 
case when b.MG4='040' then 'pass'else 'error' end exception_type,
'E-A-MVGR2_MVGR4-01' exception_code,a.dt
from tmp_materialmaster_marc a
join tmp_ods_dq_mvke b 
on a.material =b.material
where a.division_type='BSC' and b.sorg ='CN10' and MG2='OTO'
union ALL 
select a.material,a.profitcenter,a.plant,a.division_type ,b.MG2 para1,b.MG4 para2, 
case when b.MG2='OTO' then 'pass'else 'error' end exception_type,
'E-A-MVGR2_MVGR4-01' exception_code,a.dt
from tmp_materialmaster_marc a
join tmp_ods_dq_mvke b 
on a.material =b.material
where a.division_type='CRM' and b.sorg ='CN50' and MG4='040';



drop table if exists tmp_dq_P47;
create table  tmp_dq_P47 stored as orc as
--P47 MARA表中No.字段, 通过监控每个MARA版本的数值变化, 将变化部分显示出来即可。 
select a.material ,a.no old_material_no,b.no,a.last_chg, 
case when a.no <> b.no then 'warning' else 'pass' end exception_type,
'W-A-BLANZ-01' exception_code,a.dt
from opsdw.dwd_dq_mara a
join (select material,no from opsdw.dwd_dq_mara
where dt='$dwd_dq_mara_m2dt' ) b
on a.material =b.material
where a.dt='$dwd_dq_mara_maxdt';
   
drop table if exists tmp_dq_P80;
create table  tmp_dq_P80 stored as orc as
--P80
select 
a.material,a.ms para1,b.plc_code para2,
case when b.plc_code not in ('S6','S7') then 'error' else 'pass' END exception_type,
'W-A-MSTAE-01' exception_code,dt
from opsdw.dwd_dq_mara a 
left join dwd_dim_material_p360 b 
on a.material =b.material 
where a.dt ='$dwd_dq_mara_maxdt' and a.ms='90' --Warning –- Global Inventory Depleted, Check PLCP Stage
union all 
select 
a.material,a.ms para1,b.plc_code para2,
case when b.plc_code not in ('S6','S7') then 'error' else 'pass' END exception_type,
'W-A-MSTAE-02' exception_code,dt
from opsdw.dwd_dq_mara a 
left join dwd_dim_material_p360 b 
on a.material =b.material 
where a.dt ='$dwd_dq_mara_maxdt' and a.ms='91'; --Warning –- Shelf Life Depleted, Check PLCP Stage

--------------------------------------------------------------------------tmp_dq_P5
insert overwrite table dwd_dq_exception partition(db_type,dt)
select 
material,
profitcenter,
plnt,
division_type,
para1,
para2,
exception_type,
exception_code,
'P5' db_type,
dt
from tmp_dq_P5 where exception_type<>'pass';

--------------------------------------------------------------------------tmp_dq_P18
insert overwrite table dwd_dq_exception partition(db_type,dt)
select 
material,
profitcenter,
plant,
division_type,
para1,
para2,
exception_type,
exception_code,
'P18' db_type,
dt
from tmp_dq_P18 where exception_type<>'pass';

--------------------------------------------------------------------------tmp_dq_P23
insert overwrite table dwd_dq_exception partition(db_type,dt)
select 
material,
'' profitcenter,
'' plant,
'' division_type,
mg1 para1,
'' para2,
exception_type,
exception_code,
'P23' db_type,
dt
from tmp_dq_P23 where exception_type<>'pass';


--------------------------------------------------------------------------tmp_dq_P25
insert overwrite table dwd_dq_exception partition(db_type,dt)
select 
material,
profitcenter,
plant,
division_type,
para1,
para2,
exception_type,
exception_code,
'P25' db_type,
dt
from tmp_dq_P25 where exception_type<>'pass';



--------------------------------------------------------------------------tmp_dq_P47
insert overwrite table dwd_dq_exception partition(db_type,dt)
select 
material,
'' profitcenter,
'' plant,
'' division_type,
old_material_no para1,
no para2,
exception_type,
exception_code,
'P47' db_type,
dt
from tmp_dq_P47 where exception_type<>'pass';

--------------------------------------------------------------------------tmp_dq_P80
insert overwrite table dwd_dq_exception partition(db_type,dt)
select 
material,
'' profitcenter,
'' plant,
'' division_type,
para1,
para2,
exception_type,
exception_code,
'P80' db_type,
dt
from tmp_dq_P80 where exception_type<>'pass';


--------------------------------------------------------------------------tmp_dq_P5
insert overwrite table dws_dq_exception partition(exception_code,dt)
select 
plnt,
division_type,
SUM(case when exception_type<>'pass' then 1 else 0 end ) exception_qty,
COUNT(1) total_upn,
exception_code,
dt
from tmp_dq_P5
group by plnt,division_type,exception_code,dt;

--------------------------------------------------------------------------tmp_dq_P18
insert overwrite table dws_dq_exception partition(exception_code,dt)
select 
plant,
division_type,
SUM(case when exception_type<>'pass' then 1 else 0 end ) exception_qty,
COUNT(1) total_upn,
exception_code,
dt
from tmp_dq_P18
group by plant,division_type,exception_code,dt;

--------------------------------------------------------------------------tmp_dq_P23
insert overwrite table dws_dq_exception partition(exception_code,dt)
select 
'' plant,
'' division_type,
SUM(case when exception_type<>'pass' then 1 else 0 end ) exception_qty,
COUNT(1) total_upn,
exception_code,
dt
from tmp_dq_P23
group by exception_code,dt;


--------------------------------------------------------------------------tmp_dq_P25
insert overwrite table dws_dq_exception partition(exception_code,dt)
select 
plant,
division_type,
SUM(case when exception_type<>'pass' then 1 else 0 end ) exception_qty,
COUNT(1) total_upn,
exception_code,
dt
from tmp_dq_P25
group by plant,division_type,exception_code,dt;


--------------------------------------------------------------------------tmp_dq_P47
insert overwrite table dws_dq_exception partition(exception_code,dt)
select 
'' plant,
'' division_type,
SUM(case when exception_type<>'pass' then 1 else 0 end ) exception_qty,
COUNT(1) total_upn,
exception_code,
dt
from tmp_dq_P47
group by exception_code,dt;

--------------------------------------------------------------------------tmp_dq_P80
insert overwrite table dws_dq_exception partition(exception_code,dt)
select 
'' plant,
'' division_type,
SUM(case when exception_type<>'pass' then 1 else 0 end ) exception_qty,
COUNT(1) total_upn,
exception_code,
dt
from tmp_dq_P80
group by exception_code,dt;

"


delete_tmp="
--drop table tmp_materialmaster_marc;
"
# 2. 执行加载数据SQL
echo "$sto_sql"
$hive -e "$sto_sql"
#第二部分收尾删除所有临时表
echo "two $delete_tmp"
# $hive -e "$delete_tmp"
echo "End syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} .................."