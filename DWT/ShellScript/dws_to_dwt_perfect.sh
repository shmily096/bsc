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
    last_month=$(date -d "$(date  +'%Y%m')01 last month" +'%Y%m')
fi

# 获取下个月的日期（加上一个月）  
next_month_date=$(date -d "$current_date +1 month" +"%F") 

echo "start syncing dwd_trans_csgn_clear data into DWS layer on ${sync_date} : ${sync_date[year]}"
# dwd_dim_material_maxdt=`hdfs dfs -ls /bsc/opsdw/dwd/dwd_dim_material | tail -n 1 | awk -F'=' '{print $NF}'`
ods_dq_mvke_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_dq_mvke | tail -n 1 | awk -F'=' '{print $NF}'`
controllist_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_perfectorder_controllist | tail -n 1 | awk -F'=' '{print $NF}'`
confirmedreason_maxdt=`hdfs dfs -ls /bsc/opsdw/ods/ods_perfectorder_confirmedreason | tail -n 1 | awk -F'=' '{print $NF}'`


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

drop table if exists tmp_dwd_perfectorder;
create table  tmp_dwd_perfectorder stored as orc as 
SELECT 
	displayname as bu,
	get_json_object(jsons, '$.customer_code') AS customer_code,
	substring(min(a.dt) over(partition by order_no),0,7) as year_mon,
	order_no as so_no, 
	delivery_id, 
	plant ,
	case when a.plant =m.plnt then 0 else 1 end plant_type,
	get_json_object(jsons, '$.dn_line_ct') AS line_no,
	get_json_object(jsons, '$.po_number') AS po_number,
	a.material ,
	a.dt as dn_dt,
	substring(get_json_object(jsons, '$.so_created_datetime'),0,10) as so_dt,
	a.value as so_qty 
	FROM dws_finance_upn_qty a 
	left join 
	(select material,plnt from ods_dq_mvke where dt='$ods_dq_mvke_maxdt') m
	on a.material=m.material
WHERE 1=1 and a.category ='outbound_delivery_qty'
and a.order_type in ('KB','OR','ZNC') 
and get_json_object(jsons, '$.order_status') ='C'
and get_json_object(jsons, '$.so_year_mon') >='$last_month';

drop table if exists tmp_dwd_perfectsummary;
create table  tmp_dwd_perfectsummary stored as orc as 
select year_mon,bu, so_no,
--sum(plant_type) sum_plant_type,  
count(distinct plant) plant_num,count(distinct dn_dt) dt_num,
sum(cast(line_no as int)) line_qty,sum(cast(so_qty as float)) so_qty
from tmp_dwd_perfectorder 
group by year_mon,bu, so_no;


insert overwrite table dwd_perfect_detail partition(year_mon)
select 
a.bu,a.customer_code, a.so_no,a.delivery_id ,a.plant,a.plant_type,a.line_no,
a.po_number,a.material,a.dn_dt,a.so_dt,a.so_qty,b.dt_num,
case 
when dt_num>1 and a.so_dt=a.dn_dt then 'Reason 0'
when dt_num>1 and c.ordertype in ('ZNC','FD','OR','KB')  then 'Reason 1'
when a.plant_type>0 and plant_num>1 and dt_num>1 then 'Reason 2'  --非默认仓plant_type>0
when a.plant_type=0 and plant_num>1 and dt_num>1 then 'Reason 3'  --默认仓plant_type=0
when plant_num=1 and dt_num>1 then 'Reason 4'
when dt_num>1 and b.so_qty>=700 then 'Reason 5'
when dt_num>1 and SUBSTR(po_number,-2)='CR' and line_qty>200 then 'Reason 6'
when dt_num=1  then 'perfect order'
else '' end casetype,
d.adjust_casetype,
a.year_mon
from
tmp_dwd_perfectorder a 
join tmp_dwd_perfectsummary b on 
a.so_no =b.so_no and a.year_mon =b.year_mon
left join 
(select salesdocument ,upn ,ordertype 
from ods_perfectorder_controllist where updatemon >='$last_month' and if_control ='In Control') c 
on a.so_no =c.salesdocument and a.material =c.upn
left join 
(select so_no,delivery_id,adjust_casetype 
from ods_perfectorder_confirmedreason 
where updatemon ='$confirmedreason_maxdt' and year_mon='$last_month') d 
on a.so_no =d.so_no and a.delivery_id =d.delivery_id;
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