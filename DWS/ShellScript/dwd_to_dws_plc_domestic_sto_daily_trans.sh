#!/bin/bash
# Function:
#   sync up domestic_sto of product life cycle 
# History:
# 2021-06-29    Donny   v1.0    init

# 设置必要的参数
target_db_name='opsdw' # 数据加载目标数据库名称
hive=/opt/module/hive3/bin/hive  # Hive的配置路径
hadoop=/opt/module/hadoop3/bin/hadoop # Hadoop的配置路径
default_dt=$(date  +%F)

if [ -n "$1" ] ;then 
    sync_date=$1
else
    sync_date=$(date  +%F)
fi


echo "start syncing data into dws layer on $sync_date .................."



sql_str="
use ${target_db_name};
-- 配置参数
set mapreduce.job.queuename=default;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;
--set hive.exec.max.created.files=100000;
--set parquet.memory.min.chunk.size=100000;
--set hive.input.format=org.apache.hadoop.hive.ql.io.hiveinputformat;
--set hive.exec.reducers.max=8;
--set mapred.reduce.tasks=8;
--set hive.exec.parallel=false;


drop table if exists tmp_so_sto_wo;
create table tmp_so_sto_wo stored as orc as
select 
        material
        ,batch
        ,qr_code
        ,domestic_sto_dn
        ,domestic_sto
        ,dt
    from dws_so_sto_wo_daily_trans
    where  dt>=date_add('$sync_date',-7)
	and dt<='$sync_date'
    group by 
        material
        ,batch
        ,qr_code
        ,domestic_sto_dn
        ,domestic_sto
        ,dt;

drop table if exists tmp_dom_dn;
create table tmp_dom_dn stored as orc as
    select  delivery_no 
           ,sto_no 
           ,actual_putaway_datetime   as domestic_putaway 
           ,create_datetime           as domestic_dn_create_dt 
           ,actua_good_issue_datetime as domestic_pgi 
           ,actual_migo_date          as domestic_migo
    from dwd_fact_domestic_sto_dn_info
    where dt >=date_add('$sync_date',-300)
	and dt<='$sync_date'
    group by
          delivery_no 
           ,sto_no 
           ,actual_putaway_datetime
           ,create_datetime
           ,actua_good_issue_datetime
           ,actual_migo_date;
		   
drop table if exists tmp_dom_sto;
create table tmp_dom_sto stored as orc as
    select  sto_no
           ,create_datetime as domestic_sto_create_dt
    from dwd_fact_domestic_sto_info
    where dt >=date_add('$sync_date',-300)
	and dt<='$sync_date'
    group by
           sto_no
           ,create_datetime;

drop table if exists tmp_dws_plc_domestic_sto_daily_trans;
create table tmp_dws_plc_domestic_sto_daily_trans stored as orc as
select  distinct 
    so_sto_wo.material 
    ,so_sto_wo.batch
    ,so_sto_wo.qr_code 
    ,so_sto_wo.domestic_sto
    ,so_sto_wo.domestic_sto_dn
    ,dom_dn.domestic_dn_create_dt
    ,dom_dn.domestic_pgi
    ,dom_dn.domestic_migo
    ,dom_dn.domestic_putaway
    ,so_sto_wo.dt
from tmp_so_sto_wo so_sto_wo
left join tmp_dom_dn dom_dn	  
	on so_sto_wo.domestic_sto_dn=dom_dn.delivery_no 
	and so_sto_wo.domestic_sto=dom_dn.sto_no;
---删除临时表		
drop table tmp_so_sto_wo;
drop table tmp_dom_dn;

insert overwrite table dws_plc_domestic_sto_daily_trans partition(dt)
select  distinct 
    so_sto_wo.material 
    ,so_sto_wo.batch
    ,so_sto_wo.qr_code 
    ,so_sto_wo.domestic_sto
    ,so_sto_wo.domestic_sto_dn
    ,dom_sto.domestic_sto_create_dt
    ,so_sto_wo.domestic_dn_create_dt
    ,so_sto_wo.domestic_pgi
    ,so_sto_wo.domestic_migo
    ,so_sto_wo.domestic_putaway
    ,so_sto_wo.dt
from tmp_dws_plc_domestic_sto_daily_trans so_sto_wo
left join tmp_dom_sto dom_sto on dom_sto.sto_no=so_sto_wo.domestic_sto; 
"
delete_sql="
drop table tmp_dws_plc_domestic_sto_daily_trans;
drop table tmp_dom_sto;
"
# 2. 执行加载数据SQL
echo "$sql_str"
$hive -e "$sql_str"
$hive -e "$delete_sql"

echo "End syncing data into DWS dws_plc_domestic_sto_daily_trans layer on  $sync_date .................."